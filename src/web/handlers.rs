use super::schema::{
        Channel, ChannelIdType, ChannelsList, LogsParams,
        UserIdType, UserLogPathParams,
    };
use crate::logs::schema::message::{basic::BasicMessage, ResponseMessage};
use crate::{
    app::App,
    db::schema::StructuredMessage,
    Result,
};
use aide::axum::IntoApiResponse;
use axum::{
    extract::{
        ws::{Message, WebSocket}, Query, State, WebSocketUpgrade,
    },
    response::IntoResponse,
    Json,
};
use axum_extra::{headers::CacheControl, TypedHeader};
use futures::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use rand::{distr::Alphanumeric, rng, Rng};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::debug;

lazy_static! {
    static ref FIREHOSE_CLIENTS_GAUGE: IntGauge = register_int_gauge!(
        "rustlog_firehose_clients_count",
        "How many firehose clients are connected to the websocket",
    )
    .unwrap();
}

pub async fn get_channels(app: State<App>) -> impl IntoApiResponse {
    let channel_ids = app.config.channels.read().unwrap().clone();

    let channels = app
        .get_users(Vec::from_iter(channel_ids), vec![], false)
        .await
        .unwrap();

    let json = Json(ChannelsList {
        channels: channels
            .into_iter()
            .map(|(user_id, name)| Channel { name, user_id })
            .collect(),
    });
    (cache_header(600), json)
}

pub async fn firehose(
    app: State<App>,
    ws: WebSocketUpgrade,
    Query(logs_params): Query<LogsParams>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        firehose_socket(socket, app.firehose_tx.subscribe(), logs_params.json_basic)
    })
}

async fn firehose_socket(
    socket: WebSocket,
    mut firehose_rx: broadcast::Receiver<StructuredMessage<'static>>,
    json_basic: bool,
) {
    let (mut sender, mut receiver) = socket.split();

    let mut send_task = tokio::spawn(async move {
        while let Ok(message) = firehose_rx.recv().await {
            let raw_message = if json_basic {
                match BasicMessage::from_structured(&message) {
                    Ok(basic_msg) => match serde_json::to_string(&basic_msg) {
                        Ok(json) => json,
                        Err(err) => {
                            debug!("Failed to serialize BasicMessage: {}", err);
                            continue;
                        }
                    },
                    Err(err) => {
                        debug!("Failed to convert to BasicMessage: {}", err);
                        continue;
                    }
                }
            } else {
                message.to_raw_irc()
            };

            debug!("Sending message on firehose: {}", raw_message);
            if sender
                .send(Message::Text(raw_message.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(_)) = receiver.next().await {
            debug!("Received message on websocket");
        }
    });

    debug!("Websocket connected");
    FIREHOSE_CLIENTS_GAUGE.inc();

    tokio::select! {
        _ = &mut send_task => {},
        _ = &mut recv_task => {},
    }

    debug!("Websocket closed");
    FIREHOSE_CLIENTS_GAUGE.dec();
}

pub async fn optout(app: State<App>) -> Json<String> {
    let mut rng = rng();
    let optout_code: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();

    app.optout_codes.insert(optout_code.clone());

    {
        let codes = app.optout_codes.clone();
        let optout_code = optout_code.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if codes.remove(&optout_code).is_some() {
                debug!("Dropping optout code {optout_code}");
            }
        });
    }

    Json(optout_code)
}

fn cache_header(secs: u64) -> TypedHeader<CacheControl> {
    TypedHeader(
        CacheControl::new()
            .with_public()
            .with_max_age(Duration::from_secs(secs)),
    )
}

pub fn no_cache_header() -> TypedHeader<CacheControl> {
    TypedHeader(CacheControl::new().with_no_cache())
}

async fn resolve_user_params(params: &UserLogPathParams, app: &App) -> Result<(String, String)> {
    let channel_id = match params.channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&params.channel).await?,
        ChannelIdType::Id => params.channel.clone(),
    };
    let user_id = match params.user_id_type {
        UserIdType::Name => app.get_user_id_by_name(&params.user).await?,
        UserIdType::Id => params.user.clone(),
    };
    Ok((channel_id, user_id))
}

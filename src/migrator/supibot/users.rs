use anyhow::{anyhow, Context};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, warn};

#[derive(Default)]
pub struct UsersClient {
    client: reqwest::Client,
    users: HashMap<String, IvrUser>,
    // Names mapped to ids
    names: HashMap<String, Option<String>>,
}

impl UsersClient {
    pub async fn get_users(
        &mut self,
        ids: &[impl AsRef<str>],
    ) -> anyhow::Result<HashMap<String, IvrUser>> {
        let mut ids_to_request = Vec::with_capacity(ids.len());
        let mut response_users = HashMap::with_capacity(ids.len());

        for id in ids {
            match self.users.get(id.as_ref()) {
                Some(user) => {
                    response_users.insert(user.id.clone(), user.clone());
                }
                None => {
                    ids_to_request.push(id.as_ref());
                }
            }
        }

        let request_futures = ids_to_request.chunks(50).map(|chunk| {
            debug!("Requesting a chunk of {} users", chunk.len());

            async {
                let response = self
                    .client
                    .get("https://api.ivr.fi/v2/twitch/user")
                    .query(&[("id", chunk.join(","))])
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Got an error from IVR API: {} {}",
                        response.status(),
                        response.text().await?
                    ));
                }
                Ok(response.json::<Vec<IvrUser>>().await?)
            }
        });
        // let results = join_all(request_futures).await;
        let mut results = Vec::new();
        for future in request_futures {
            results.push(future.await);
        }

        for result in results {
            let api_response = result?;
            for user in api_response {
                self.users.insert(user.id.clone(), user.clone());
                response_users.insert(user.id.clone(), user);
            }
        }

        if !ids_to_request.is_empty() {}

        Ok(response_users)
    }

    pub async fn get_user(&mut self, id: &str) -> anyhow::Result<IvrUser> {
        let users = self.get_users(&[id]).await?;
        users.into_values().next().context("Empty ivr response")
    }

    pub async fn get_user_by_name(&mut self, name: &str) -> anyhow::Result<Option<IvrUser>> {
        match self.names.get(name) {
            Some(id) => Ok(id.as_ref().map(|id| self.users.get(id).cloned().unwrap())),
            None => {
                debug!("Fetching info for name {name}");
                let response = self
                    .client
                    .get("https://api.ivr.fi/v2/twitch/user")
                    .query(&[("login", name)])
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Got an error from IVR API: {} {}",
                        response.status(),
                        response.text().await?
                    ));
                }

                let users: Vec<IvrUser> = response
                    .json()
                    .await
                    .context("Could not deserialize IVR response")?;

                match users.into_iter().next() {
                    Some(user) => {
                        self.names.insert(user.login.clone(), Some(user.id.clone()));
                        self.users.insert(user.id.clone(), user.clone());
                        Ok(Some(user))
                    }
                    None => {
                        warn!("User {name} cannot be retrieved");
                        self.names.insert(name.to_owned(), None);
                        Ok(None)
                    }
                }
            }
        }
    }

    pub fn get_cached_user(&self, id: &str) -> Option<&IvrUser> {
        self.users.get(id)
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IvrUser {
    pub id: String,
    pub display_name: String,
    pub login: String,
    pub chat_color: Option<String>,
}
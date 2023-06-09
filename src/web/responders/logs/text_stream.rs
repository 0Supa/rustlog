use super::join_iter::JoinIter;
use crate::{
    logs::{parse_messages, parse_raw, stream::LogsStream},
    Result,
};
use futures::{stream::TryChunks, Future, Stream, StreamExt, TryStreamExt};
use rayon::prelude::ParallelIterator;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;

const CHUNK_SIZE: usize = 3000;

pub struct TextLogsStream {
    inner: TryChunks<LogsStream>,
}

impl TextLogsStream {
    pub fn new(stream: LogsStream) -> Self {
        let inner = stream.try_chunks(CHUNK_SIZE);
        Self { inner }
    }
}

impl Stream for TextLogsStream {
    type Item = Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.inner.next();
        pin!(fut);

        match fut.poll(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(chunk) => {
                    let irc_messages = parse_raw(chunk);
                    let messages: Vec<_> = parse_messages(&irc_messages).collect();

                    let mut text = messages.iter().join('\n').to_string();
                    text.push('\n');

                    Poll::Ready(Some(Ok(text)))
                }
                Err(err) => Poll::Ready(Some(Err(err.1))),
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

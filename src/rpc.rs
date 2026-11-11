use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::UnixStream;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TurnComplete {
    pub outcome: String,
    pub content: String,
}

pub struct RpcClient {
    lines: Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    next_id: u64,
}

impl RpcClient {
    pub async fn connect(path: &Path, env: HashMap<String, String>) -> Result<Self> {
        let stream = UnixStream::connect(path)
            .await
            .with_context(|| format!("connect to ZeroClaw RPC socket '{}'", path.display()))?;
        let (reader, writer) = tokio::io::split(stream);
        Self::from_io(Box::new(reader), Box::new(writer), env).await
    }

    async fn from_io(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        env: HashMap<String, String>,
    ) -> Result<Self> {
        let mut client = Self {
            lines: BufReader::new(reader).lines(),
            writer,
            next_id: 1,
        };
        client
            .call("initialize", json!({"protocol_version": 1, "env": env}))
            .await
            .context("initialize ZeroClaw RPC connection")?;
        Ok(client)
    }

    pub async fn session_new(
        &mut self,
        agent_alias: &str,
        session_id: &str,
        cwd: &Path,
    ) -> Result<()> {
        self.call(
            "session/new",
            json!({
                "agent_alias": agent_alias,
                "cwd": cwd,
                "session_id": session_id,
                "keep_siblings": true
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn session_state(&mut self, session_id: &str) -> Result<String> {
        let value = self
            .call("session/state", json!({"session_id": session_id}))
            .await?;
        value["state"]
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("session/state response has no state"))
    }

    pub async fn session_abort(&mut self, session_id: &str) -> Result<()> {
        self.call("session/abort", json!({"session_id": session_id}))
            .await?;
        Ok(())
    }

    pub async fn session_prompt<F>(
        &mut self,
        session_id: &str,
        prompt: &str,
        mut observe: F,
    ) -> Result<TurnComplete>
    where
        F: FnMut(&Value) -> Result<()>,
    {
        let request_id = self
            .send_request(
                "session/prompt",
                json!({"session_id": session_id, "prompt": prompt}),
            )
            .await?;
        loop {
            let frame = self.read_frame().await?;
            observe(&frame)?;
            if frame.get("id").and_then(Value::as_u64) == Some(request_id)
                && let Some(error) = frame.get("error")
                && !error.is_null()
            {
                bail!("session/prompt failed: {}", rpc_error_message(error));
            }
            if frame.get("method").and_then(Value::as_str) == Some("session/update")
                && frame["params"]["type"] == "turn_complete"
                && frame["params"]["session_id"] == session_id
            {
                return serde_json::from_value(frame["params"].clone())
                    .context("decode terminal worker event");
            }
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.send_request(method, params).await?;
        loop {
            let frame = self.read_frame().await?;
            if frame.get("id").and_then(Value::as_u64) != Some(request_id) {
                continue;
            }
            if let Some(error) = frame.get("error")
                && !error.is_null()
            {
                bail!("{method} failed: {}", rpc_error_message(error));
            }
            return Ok(frame.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    async fn send_request(&mut self, method: &str, params: Value) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        let mut encoded = serde_json::to_vec(&request)?;
        encoded.push(b'\n');
        self.writer.write_all(&encoded).await?;
        self.writer.flush().await?;
        Ok(id)
    }

    async fn read_frame(&mut self) -> Result<Value> {
        let line = self
            .lines
            .next_line()
            .await?
            .ok_or_else(|| anyhow!("ZeroClaw RPC connection closed"))?;
        serde_json::from_str(&line).context("decode ZeroClaw RPC frame")
    }
}

fn rpc_error_message(error: &Value) -> String {
    error["message"]
        .as_str()
        .unwrap_or("unknown JSON-RPC error")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_request<R: AsyncRead + Unpin>(lines: &mut Lines<BufReader<R>>) -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) {
        let mut encoded = serde_json::to_vec(&value).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn prompt_finishes_on_turn_complete_before_legacy_response() {
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let server = tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(server_stream);
            let mut lines = BufReader::new(reader).lines();
            let _ = read_request(&mut lines).await;
            send(&mut writer, json!({"jsonrpc":"2.0","id":1,"result":{}})).await;
            let prompt = read_request(&mut lines).await;
            assert_eq!(prompt["method"], "session/prompt");
            send(
                &mut writer,
                json!({"jsonrpc":"2.0","method":"session/update","params":{"type":"turn_complete","session_id":"worker","outcome":"completed","content":"done"}}),
            )
            .await;
        });

        let (reader, writer) = tokio::io::split(client_stream);
        let mut client = RpcClient::from_io(Box::new(reader), Box::new(writer), HashMap::new())
            .await
            .unwrap();
        let turn = client
            .session_prompt("worker", "do it", |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(turn.content, "done");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_session_lifecycle_matches_zeroclaw_rpc() {
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let server = tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(server_stream);
            let mut lines = BufReader::new(reader).lines();
            let init = read_request(&mut lines).await;
            assert_eq!(init["params"]["env"]["HOME"], "/task/home");
            send(&mut writer, json!({"jsonrpc":"2.0","id":1,"result":{}})).await;

            let new = read_request(&mut lines).await;
            assert_eq!(new["method"], "session/new");
            assert_eq!(new["params"]["agent_alias"], "bobr_worker");
            assert_eq!(new["params"]["cwd"], "/task");
            assert_eq!(new["params"]["keep_siblings"], true);
            send(&mut writer, json!({"jsonrpc":"2.0","id":2,"result":{}})).await;

            let state = read_request(&mut lines).await;
            assert_eq!(state["method"], "session/state");
            send(
                &mut writer,
                json!({"jsonrpc":"2.0","id":3,"result":{"state":"running"}}),
            )
            .await;

            let abort = read_request(&mut lines).await;
            assert_eq!(abort["method"], "session/abort");
            send(&mut writer, json!({"jsonrpc":"2.0","id":4,"result":{}})).await;
        });

        let (reader, writer) = tokio::io::split(client_stream);
        let mut env = HashMap::new();
        env.insert("HOME".into(), "/task/home".into());
        let mut client = RpcClient::from_io(Box::new(reader), Box::new(writer), env)
            .await
            .unwrap();
        client
            .session_new("bobr_worker", "worker", Path::new("/task"))
            .await
            .unwrap();
        assert_eq!(client.session_state("worker").await.unwrap(), "running");
        client.session_abort("worker").await.unwrap();
        server.await.unwrap();
    }
}

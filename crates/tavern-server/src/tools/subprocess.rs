use std::collections::HashMap;
use std::process::Stdio;

use serde_json::Value;
use tavern_core::{ToolError, ToolHandler, ToolResult};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const MAX_STDOUT_BYTES: usize = 10 * 1024 * 1024; // 10 MB

/// 子进程工具执行器。通过 stdin/stdout JSON 协议与外部进程通信。
pub struct SubprocessHandler {
    command: String,
    timeout_ms: u64,
    cwd: Option<String>,
    env: Option<HashMap<String, String>>,
}

impl SubprocessHandler {
    pub fn new(
        command: &str,
        timeout_ms: u64,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
    ) -> Self {
        Self {
            command: command.to_string(),
            timeout_ms,
            cwd: cwd.map(|s| s.to_string()),
            env: env.map(|m| m.clone()),
        }
    }

    fn parse_command(&self) -> (&str, Vec<&str>) {
        let mut parts = self.command.split_whitespace();
        let prog = parts.next().unwrap_or("");
        let args: Vec<&str> = parts.collect();
        (prog, args)
    }
}

#[async_trait::async_trait]
impl ToolHandler for SubprocessHandler {
    async fn execute(
        &self,
        params: Value,
        tenant_id: &str,
        session_id: &str,
        tool_call_id: &str,
    ) -> Result<ToolResult, ToolError> {
        let request = serde_json::json!({
            "params": params,
            "tool_call_id": tool_call_id,
            "session_id": session_id,
            "tenant_id": tenant_id,
        });
        let request_json =
            serde_json::to_string(&request).map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;

        let (prog, args) = self.parse_command();
        let mut cmd = Command::new(prog);
        cmd.args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(ref cwd) = self.cwd {
            cmd.current_dir(cwd);
        }

        match &self.env {
            Some(env_map) => {
                cmd.env_clear();
                for (k, v) in env_map {
                    cmd.env(k, v);
                }
            }
            None => {} // 继承 server 环境
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| ToolError::ExecutionFailed(format!("failed to spawn: {}", e)))?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(request_json.as_bytes())
                .await
                .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
            // stdin 在 drop 时自动关闭，通知子进程 EOF
        }

        let timeout = std::time::Duration::from_millis(self.timeout_ms);
        let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                return Err(ToolError::ExecutionFailed(format!(
                    "child process error: {}",
                    e
                )));
            }
            Err(_) => {
                // 超时——kill_on_drop 已设置，child drop 时自动 kill
                return Ok(ToolResult {
                    content: vec![tavern_core::ContentPart {
                        content_type: "text".into(),
                        text: Some(format!(
                            "tool execution timed out after {}ms",
                            self.timeout_ms
                        )),
                    }],
                    is_error: true,
                    details: None,
                });
            }
        };

        if output.stdout.len() > MAX_STDOUT_BYTES {
            return Ok(ToolResult {
                content: vec![tavern_core::ContentPart {
                    content_type: "text".into(),
                    text: Some(format!(
                        "tool output exceeded {}MB limit",
                        MAX_STDOUT_BYTES / (1024 * 1024)
                    )),
                }],
                is_error: true,
                details: None,
            });
        }

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Ok(ToolResult {
                content: vec![tavern_core::ContentPart {
                    content_type: "text".into(),
                    text: Some(format!(
                        "tool exited with code {}: {}",
                        output.status.code().unwrap_or(-1),
                        stderr
                    )),
                }],
                is_error: true,
                details: None,
            });
        }

        if !output.stderr.is_empty() {
            tracing::debug!(
                stderr = %String::from_utf8_lossy(&output.stderr),
                "subprocess tool wrote to stderr"
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let result: serde_json::Value = serde_json::from_str(&stdout).map_err(|e| {
            ToolError::ExecutionFailed(format!("invalid JSON from tool: {}: {}", e, stdout))
        })?;
        serde_json::from_value(result).map_err(|e| ToolError::ExecutionFailed(e.to_string()))
    }
}

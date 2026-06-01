use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

/// Tavern 应用完整配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TavernConfig {
    #[serde(default = "default_server")]
    pub server: ServerConfig,
    #[serde(default = "default_auth")]
    pub auth: AuthConfig,
    #[serde(default = "default_store")]
    pub store: StoreConfig,
    #[serde(default = "default_observability")]
    pub observability: ObservabilityConfig,
    #[serde(default = "default_runtime")]
    pub runtime: RuntimeConfig,
    #[serde(default = "default_reload")]
    pub reload: ReloadConfig,
    /// V0.3.2: 租户限流配置
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl TavernConfig {
    /// 从配置文件 + 环境变量加载配置。
    /// 优先级（高 → 低）：
    /// 1. `TAVERN_*` 环境变量
    /// 2. `config.toml`
    /// 3. V0.1.0 兼容环境变量（RUNTIME_URL 等）
    /// 4. 默认值
    #[allow(clippy::result_large_err)]
    pub fn load() -> Result<Self, figment::Error> {
        let mut figment = Figment::new();

        // 1. 默认值
        figment = figment.merge(figment::providers::Serialized::defaults(
            TavernConfig::default(),
        ));

        // 2. config.toml（如果存在）
        let config_path =
            std::env::var("TAVERN_CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
        if std::path::Path::new(&config_path).exists() {
            figment = figment.merge(Toml::file(&config_path));
        }

        // 3. TAVERN_* 环境变量
        figment = figment.merge(Env::prefixed("TAVERN_").split("__"));

        // 4. V0.1.0 兼容环境变量
        figment = figment.merge(Env::raw().only(&[
            "RUNTIME_URL",
            "AGENT_CONFIG_DIR",
            "SERVER_HOST",
            "SERVER_PORT",
            "WORKFLOW_CONFIG_DIR",
            "MAX_WORKFLOW_CONCURRENCY",
        ]));

        let mut config: TavernConfig = figment.extract()?;

        // V0.1.0 兼容回退
        if config.runtime.url.is_empty() {
            if let Ok(url) = std::env::var("RUNTIME_URL") {
                config.runtime.url = url;
            }
        }

        Ok(config)
    }
}

impl Default for TavernConfig {
    fn default() -> Self {
        Self {
            server: default_server(),
            auth: default_auth(),
            store: default_store(),
            observability: default_observability(),
            runtime: default_runtime(),
            reload: default_reload(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_agent_config_dir")]
    pub agent_config_dir: String,
    #[serde(default = "default_workflow_config_dir")]
    pub workflow_config_dir: String,
    #[serde(default = "default_max_concurrency")]
    pub max_workflow_concurrency: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    #[serde(default = "default_auth_type")]
    pub auth_type: String,
    #[serde(default, deserialize_with = "deserialize_comma_separated")]
    pub keys: Vec<String>,
    #[serde(default = "default_jwt_secret")]
    pub jwt_secret: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreConfig {
    #[serde(default = "default_store_type")]
    pub store_type: String,
    #[serde(default = "default_database_url")]
    pub database_url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_metrics_public")]
    pub metrics_public: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReloadConfig {
    #[serde(default = "default_reload_enabled")]
    pub enabled: bool,
    #[serde(default = "default_reload_debounce_ms")]
    pub debounce_ms: u64,
}

// ── V0.3.2: 租户限流配置 ──

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_rps")]
    pub default_rps: u32,
    #[serde(default)]
    pub per_tenant: std::collections::HashMap<String, TenantRateLimit>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_rps: default_rps(),
            per_tenant: std::collections::HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TenantRateLimit {
    pub rps: u32,
}

fn default_rps() -> u32 {
    10
}

// ── 默认值辅助函数 ──

fn default_server() -> ServerConfig {
    ServerConfig {
        host: default_host(),
        port: default_port(),
        agent_config_dir: default_agent_config_dir(),
        workflow_config_dir: default_workflow_config_dir(),
        max_workflow_concurrency: default_max_concurrency(),
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    3000
}

fn default_agent_config_dir() -> String {
    "./configs/agents".to_string()
}

fn default_workflow_config_dir() -> String {
    "./configs/workflows".to_string()
}

fn default_max_concurrency() -> usize {
    usize::MAX
}

fn default_auth() -> AuthConfig {
    AuthConfig {
        auth_type: default_auth_type(),
        keys: Vec::new(),
        jwt_secret: default_jwt_secret(),
    }
}

fn default_auth_type() -> String {
    "none".to_string()
}

fn default_jwt_secret() -> String {
    std::env::var("TAVERN_JWT_SECRET").unwrap_or_else(|_| "change-me".to_string())
}

fn default_store() -> StoreConfig {
    StoreConfig {
        store_type: default_store_type(),
        database_url: default_database_url(),
    }
}

fn default_store_type() -> String {
    "memory".to_string()
}

fn default_database_url() -> String {
    "sqlite://tavern.db".to_string()
}

fn default_observability() -> ObservabilityConfig {
    ObservabilityConfig {
        log_format: default_log_format(),
        metrics_public: default_metrics_public(),
    }
}

fn default_log_format() -> String {
    "pretty".to_string()
}

fn default_metrics_public() -> bool {
    true
}

fn default_runtime() -> RuntimeConfig {
    RuntimeConfig { url: String::new() }
}

fn default_reload() -> ReloadConfig {
    ReloadConfig {
        enabled: default_reload_enabled(),
        debounce_ms: default_reload_debounce_ms(),
    }
}

fn default_reload_enabled() -> bool {
    true
}

fn default_reload_debounce_ms() -> u64 {
    500
}

// ── 自定义反序列化 ──

fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct CommaSeparatedVisitor;

    impl<'de> Visitor<'de> for CommaSeparatedVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a comma-separated string or a list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.split(',').map(|s| s.trim().to_string()).collect())
        }

        fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
        }
    }

    deserializer.deserialize_any(CommaSeparatedVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TavernConfig::default();
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 3000);
        assert_eq!(config.auth.auth_type, "none");
        assert!(config.auth.keys.is_empty());
        assert_eq!(config.store.store_type, "memory");
        assert_eq!(config.observability.log_format, "pretty");
        assert!(config.observability.metrics_public);
        assert_eq!(config.reload.debounce_ms, 500);
    }

    #[test]
    fn test_comma_separated_keys_from_toml() {
        let toml = r#"
[auth]
auth_type = "api_key"
keys = "a, b, c"
"#;
        let figment = Figment::new().merge(figment::providers::Toml::string(toml));
        let config: TavernConfig = figment.extract().unwrap();
        assert_eq!(config.auth.keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_load_from_toml_str() {
        let toml = r#"
[server]
host = "127.0.0.1"
port = 8080

[auth]
auth_type = "api_key"
keys = ["sk-abc", "sk-def"]
"#;

        let figment = Figment::new().merge(figment::providers::Toml::string(toml));
        let config: TavernConfig = figment.extract().unwrap();

        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.auth.auth_type, "api_key");
        assert_eq!(config.auth.keys, vec!["sk-abc", "sk-def"]);
    }
}

use std::path::{Path, PathBuf};

use tavern_core::AgentConfig;

use crate::error::TavernError;

/// 从单个 YAML 文件加载 Agent 配置。
pub fn load_agent(path: &Path) -> Result<AgentConfig, TavernError> {
    let content = std::fs::read_to_string(path)?;
    let config: AgentConfig =
        serde_yaml::from_str(&content).map_err(|e| TavernError::ConfigParse {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?;
    Ok(config)
}

/// 从目录批量加载 YAML 配置。
/// 遍历目录下所有 `.yaml` / `.yml` 文件。
/// 遇到首个错误即终止，此前已加载的配置保留在返回的 Vec 中（不回滚）。
pub fn load_from_dir(dir: &Path) -> Result<Vec<(AgentConfig, PathBuf)>, TavernError> {
    let canonical_dir = std::fs::canonicalize(dir).map_err(TavernError::Io)?;
    let mut configs = Vec::new();
    for entry in std::fs::read_dir(&canonical_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().map(is_yaml_ext).unwrap_or(false) {
            let config = load_agent(&path)?;
            configs.push((config, path));
        }
    }
    Ok(configs)
}

fn is_yaml_ext(ext: &std::ffi::OsStr) -> bool {
    ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml")
}

use std::sync::OnceLock;

use minijinja::{Environment, ErrorKind, UndefinedBehavior};
use serde_json::Value;

use crate::error::CompError;

static BASE_ENV: OnceLock<Environment> = OnceLock::new();

/// 渲染模板，将 `{{key}}` 替换为 context 中对应的值。
///
/// 若变量在 context 中不存在，返回 `CompError::MissingContextVariable`。
/// V0.2.0 使用 minijinja 作为底层引擎，支持嵌套对象访问（如 `{{obj.nested}}`）。
pub fn render_template(template: &str, context: &Value) -> Result<String, CompError> {
    let env = BASE_ENV.get_or_init(|| {
        let mut e = Environment::new();
        e.set_undefined_behavior(UndefinedBehavior::Strict);
        e.set_auto_escape_callback(|_name| minijinja::AutoEscape::None);
        e
    });

    let mut local_env = env.clone();
    local_env
        .add_template("tmpl", template)
        .map_err(|e| CompError::TemplateParse {
            reason: e.to_string(),
        })?;

    let tmpl = local_env.get_template("tmpl").expect("template just added");
    let ctx = minijinja::Value::from_serialize(context);

    tmpl.render(ctx).map_err(|e| {
        if e.kind() == ErrorKind::UndefinedError {
            CompError::MissingContextVariable {
                name: e.to_string(),
            }
        } else {
            CompError::TemplateParse {
                reason: e.to_string(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_render_simple() {
        let template = "研究主题: {{topic}}";
        let context = json!({"topic": "AI Agent 框架"});
        assert_eq!(
            render_template(template, &context).unwrap(),
            "研究主题: AI Agent 框架"
        );
    }

    #[test]
    fn test_render_multiple_vars() {
        let template = "{{greeting}}, {{name}}!";
        let context = json!({"greeting": "Hello", "name": "World"});
        assert_eq!(
            render_template(template, &context).unwrap(),
            "Hello, World!"
        );
    }

    #[test]
    fn test_render_no_vars() {
        let template = "no variables here";
        let context = json!({});
        assert_eq!(
            render_template(template, &context).unwrap(),
            "no variables here"
        );
    }

    #[test]
    fn test_render_missing_var() {
        let template = "{{missing}}";
        let context = json!({});
        let err = render_template(template, &context).unwrap_err();
        assert!(matches!(err, CompError::MissingContextVariable { .. }));
    }

    #[test]
    fn test_render_number_value() {
        let template = "count: {{n}}";
        let context = json!({"n": 42});
        assert_eq!(render_template(template, &context).unwrap(), "count: 42");
    }

    #[test]
    fn test_render_object_value() {
        let template = "data: {{obj}}";
        let context = json!({"obj": {"a": 1}});
        assert_eq!(
            render_template(template, &context).unwrap(),
            "data: {\"a\": 1}"
        );
    }

    #[test]
    fn test_render_empty_template() {
        assert_eq!(render_template("", &json!({})).unwrap(), "");
    }

    #[test]
    fn test_render_nested_object() {
        let template = "notes: {{research.notes}}";
        let context = json!({"research": {"notes": "key findings"}});
        assert_eq!(
            render_template(template, &context).unwrap(),
            "notes: key findings"
        );
    }

    #[test]
    fn test_render_nested_missing() {
        let template = "{{research.missing}}";
        let context = json!({"research": {"notes": "key findings"}});
        let err = render_template(template, &context).unwrap_err();
        assert!(matches!(err, CompError::MissingContextVariable { .. }));
    }

    #[test]
    fn test_render_jinja_filter() {
        let template = "{{ name | upper }}";
        let context = json!({"name": "hello"});
        assert_eq!(render_template(template, &context).unwrap(), "HELLO");
    }
}

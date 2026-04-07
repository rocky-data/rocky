use std::collections::HashMap;

use super::webhook::WebhookConfig;

// ---------------------------------------------------------------------------
// WebhookPreset
// ---------------------------------------------------------------------------

/// A built-in webhook preset that provides sensible defaults for popular services.
///
/// User-provided fields in `WebhookConfig` override preset defaults.
#[derive(Debug, Clone)]
pub struct WebhookPreset {
    pub name: &'static str,
    pub default_headers: &'static [(&'static str, &'static str)],
    pub body_template: &'static str,
    pub default_method: &'static str,
}

// ---------------------------------------------------------------------------
// Built-in presets
// ---------------------------------------------------------------------------

pub static SLACK: WebhookPreset = WebhookPreset {
    name: "slack",
    default_headers: &[("Content-Type", "application/json")],
    default_method: "POST",
    body_template: r#"{
  "text": ":bell: Rocky: {{event}} — {{pipeline}}",
  "blocks": [
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "*Event:* `{{event}}`\n*Pipeline:* {{pipeline}}{{#if model}}\n*Model:* {{model}}{{/if}}{{#if table}}\n*Table:* {{table}}{{/if}}{{#if error}}\n*Error:* {{error}}{{/if}}{{#if duration_ms}}\n*Duration:* {{duration_ms}}ms{{/if}}"
      }
    }
  ]
}"#,
};

pub static PAGERDUTY: WebhookPreset = WebhookPreset {
    name: "pagerduty",
    default_headers: &[("Content-Type", "application/json")],
    default_method: "POST",
    body_template: r#"{
  "routing_key": "{{metadata.routing_key}}",
  "event_action": "trigger",
  "payload": {
    "summary": "Rocky: {{event}} in {{pipeline}}{{#if model}} ({{model}}){{/if}}{{#if error}} — {{error}}{{/if}}",
    "severity": "{{metadata.severity}}",
    "source": "rocky",
    "component": "{{pipeline}}",
    "group": "data-pipeline"
  }
}"#,
};

pub static DATADOG: WebhookPreset = WebhookPreset {
    name: "datadog",
    default_headers: &[("Content-Type", "application/json")],
    default_method: "POST",
    body_template: r#"{
  "title": "Rocky: {{event}} — {{pipeline}}",
  "text": "Event {{event}} fired for pipeline {{pipeline}}.{{#if model}} Model: {{model}}.{{/if}}{{#if error}} Error: {{error}}.{{/if}}",
  "alert_type": "{{#if error}}error{{/if}}{{#if metadata.severity}}{{metadata.severity}}{{/if}}",
  "source_type_name": "rocky",
  "tags": ["pipeline:{{pipeline}}", "event:{{event}}"]
}"#,
};

pub static TEAMS: WebhookPreset = WebhookPreset {
    name: "teams",
    default_headers: &[("Content-Type", "application/json")],
    default_method: "POST",
    body_template: r#"{
  "type": "message",
  "attachments": [
    {
      "contentType": "application/vnd.microsoft.card.adaptive",
      "content": {
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
          {
            "type": "TextBlock",
            "size": "Medium",
            "weight": "Bolder",
            "text": "Rocky: {{event}}"
          },
          {
            "type": "FactSet",
            "facts": [
              { "title": "Pipeline", "value": "{{pipeline}}" },
              { "title": "Event", "value": "{{event}}" }{{#if model}},
              { "title": "Model", "value": "{{model}}" }{{/if}}{{#if error}},
              { "title": "Error", "value": "{{error}}" }{{/if}}
            ]
          }
        ]
      }
    }
  ]
}"#,
};

pub static GENERIC: WebhookPreset = WebhookPreset {
    name: "generic",
    default_headers: &[("Content-Type", "application/json")],
    default_method: "POST",
    // Empty template → full JSON context is sent
    body_template: "",
};

/// All built-in presets.
pub static PRESETS: &[&WebhookPreset] = &[&SLACK, &PAGERDUTY, &DATADOG, &TEAMS, &GENERIC];

// ---------------------------------------------------------------------------
// Preset resolution
// ---------------------------------------------------------------------------

/// Looks up a preset by name (case-insensitive).
pub fn find_preset(name: &str) -> Option<&'static WebhookPreset> {
    let lower = name.to_lowercase();
    PRESETS.iter().find(|p| p.name == lower).copied()
}

/// Merges preset defaults into a `WebhookConfig`.
///
/// User-provided values always win. Preset provides:
/// - `body_template` (if user didn't specify one)
/// - `method` (if user left the default "POST")
/// - `headers` (preset headers are added, user headers override on conflict)
///
/// Returns an error string if the preset name is unknown.
pub fn resolve_preset(
    preset_name: &str,
    user_config: &WebhookConfig,
) -> Result<WebhookConfig, String> {
    let preset = find_preset(preset_name).ok_or_else(|| {
        let known: Vec<&str> = PRESETS.iter().map(|p| p.name).collect();
        format!(
            "unknown webhook preset '{}'. Known presets: {}",
            preset_name,
            known.join(", ")
        )
    })?;

    let mut resolved = user_config.clone();

    // Body template: use preset's if user didn't provide one
    // (for "generic", the empty template means "serialize full context")
    if resolved.body_template.is_none() {
        if preset.body_template.is_empty() {
            // generic preset: leave as None → full JSON serialization
        } else {
            resolved.body_template = Some(preset.body_template.to_string());
        }
    }

    // Method: only override if user left the default
    if resolved.method == "POST" {
        resolved.method = preset.default_method.to_string();
    }

    // Headers: preset defaults, user overrides on conflict
    let mut merged_headers: HashMap<String, String> = preset
        .default_headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    merged_headers.extend(resolved.headers.drain());
    resolved.headers = merged_headers;

    Ok(resolved)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::FailureAction;

    fn base_config() -> WebhookConfig {
        WebhookConfig {
            url: "https://hooks.slack.com/services/T/B/x".to_string(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            body_template: None,
            secret: None,
            timeout_ms: 10_000,
            async_mode: false,
            on_failure: FailureAction::Warn,
            retry_count: 0,
            retry_delay_ms: 1_000,
            preset: None,
        }
    }

    #[test]
    fn test_slack_preset_applies_body_template() {
        let config = base_config();
        let resolved = resolve_preset("slack", &config).unwrap();
        assert!(resolved.body_template.is_some());
        let template = resolved.body_template.unwrap();
        assert!(template.contains("{{event}}"));
        assert!(template.contains("{{pipeline}}"));
    }

    #[test]
    fn test_slack_preset_applies_headers() {
        let config = base_config();
        let resolved = resolve_preset("slack", &config).unwrap();
        assert_eq!(
            resolved.headers.get("Content-Type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_pagerduty_preset_body_template() {
        let config = base_config();
        let resolved = resolve_preset("pagerduty", &config).unwrap();
        let template = resolved.body_template.unwrap();
        assert!(template.contains("routing_key"));
        assert!(template.contains("event_action"));
        assert!(template.contains("trigger"));
    }

    #[test]
    fn test_datadog_preset() {
        let config = base_config();
        let resolved = resolve_preset("datadog", &config).unwrap();
        let template = resolved.body_template.unwrap();
        assert!(template.contains("source_type_name"));
        assert!(template.contains("rocky"));
    }

    #[test]
    fn test_teams_preset() {
        let config = base_config();
        let resolved = resolve_preset("teams", &config).unwrap();
        let template = resolved.body_template.unwrap();
        assert!(template.contains("AdaptiveCard"));
    }

    #[test]
    fn test_generic_preset_no_body_template() {
        let config = base_config();
        let resolved = resolve_preset("generic", &config).unwrap();
        // generic preset leaves body_template as None → full JSON
        assert!(resolved.body_template.is_none());
    }

    #[test]
    fn test_user_body_template_overrides_preset() {
        let mut config = base_config();
        config.body_template = Some(r#"{"custom": true}"#.to_string());
        let resolved = resolve_preset("slack", &config).unwrap();
        assert_eq!(
            resolved.body_template.as_deref(),
            Some(r#"{"custom": true}"#)
        );
    }

    #[test]
    fn test_user_headers_override_preset() {
        let mut config = base_config();
        config
            .headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let resolved = resolve_preset("slack", &config).unwrap();
        assert_eq!(resolved.headers.get("Content-Type").unwrap(), "text/plain");
    }

    #[test]
    fn test_unknown_preset_error() {
        let config = base_config();
        let result = resolve_preset("nonexistent", &config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("unknown webhook preset"));
        assert!(err.contains("nonexistent"));
        assert!(err.contains("slack"));
    }

    #[test]
    fn test_preset_case_insensitive() {
        let config = base_config();
        assert!(resolve_preset("Slack", &config).is_ok());
        assert!(resolve_preset("SLACK", &config).is_ok());
        assert!(resolve_preset("PagerDuty", &config).is_ok());
    }

    #[test]
    fn test_find_preset_returns_correct() {
        assert_eq!(find_preset("slack").unwrap().name, "slack");
        assert_eq!(find_preset("pagerduty").unwrap().name, "pagerduty");
        assert_eq!(find_preset("datadog").unwrap().name, "datadog");
        assert_eq!(find_preset("teams").unwrap().name, "teams");
        assert_eq!(find_preset("generic").unwrap().name, "generic");
        assert!(find_preset("unknown").is_none());
    }

    #[test]
    fn test_all_presets_in_list() {
        assert_eq!(PRESETS.len(), 5);
        let names: Vec<&str> = PRESETS.iter().map(|p| p.name).collect();
        assert!(names.contains(&"slack"));
        assert!(names.contains(&"pagerduty"));
        assert!(names.contains(&"datadog"));
        assert!(names.contains(&"teams"));
        assert!(names.contains(&"generic"));
    }

    #[test]
    fn test_user_config_preserved_through_resolution() {
        let mut config = base_config();
        config.secret = Some("my_secret".to_string());
        config.timeout_ms = 5000;
        config.async_mode = true;
        config.on_failure = FailureAction::Abort;
        config.retry_count = 3;
        config.retry_delay_ms = 2000;

        let resolved = resolve_preset("slack", &config).unwrap();
        assert_eq!(resolved.secret.as_deref(), Some("my_secret"));
        assert_eq!(resolved.timeout_ms, 5000);
        assert!(resolved.async_mode);
        assert_eq!(resolved.on_failure, FailureAction::Abort);
        assert_eq!(resolved.retry_count, 3);
        assert_eq!(resolved.retry_delay_ms, 2000);
    }
}

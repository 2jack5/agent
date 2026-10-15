-- Bootstrap models required by service startup before users can reach model admin.
-- Provider API keys remain empty and can be configured after login.
-- Preserve any model already configured under the default alias.
INSERT INTO model_configs (provider_id, model_name, alias, type, capabilities, is_default)
SELECT p.id, 'deepseek-chat', 'deepseek-chat', 'chat', '["function_call"]'::jsonb,
       NOT EXISTS (SELECT 1 FROM model_configs WHERE type = 'chat' AND is_default)
FROM model_providers p
WHERE p.code = 'deepseek'
  AND NOT EXISTS (SELECT 1 FROM model_configs WHERE alias = 'deepseek-chat' AND type = 'chat');

INSERT INTO model_configs (provider_id, model_name, alias, type, is_default)
SELECT p.id, 'bge-m3', 'bge-m3', 'embedding',
       NOT EXISTS (SELECT 1 FROM model_configs WHERE type = 'embedding' AND is_default)
FROM model_providers p
WHERE p.code = 'ollama'
  AND NOT EXISTS (SELECT 1 FROM model_configs WHERE alias = 'bge-m3' AND type = 'embedding');

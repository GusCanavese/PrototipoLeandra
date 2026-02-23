-- Índices focados nas consultas mais frequentes do backend.
-- Rollback simples: remover apenas os índices criados aqui.

ALTER TABLE usuarios
    ADD INDEX idx_usuarios_tipo_usuario (tipo, usuario);

ALTER TABLE chamados
    ADD INDEX idx_chamados_login_status (login_cliente, status),
    ADD INDEX idx_chamados_status_prioridade (status, prioridade);

ALTER TABLE chamado_atualizacoes
    ADD INDEX idx_chamado_atualizacoes_chamado_id (id_chamado, id);

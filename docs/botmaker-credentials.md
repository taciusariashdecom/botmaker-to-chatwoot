# Credenciais Botmaker (Ambiente de Teste)

> ⚠️ **Atenção:** mantenha estas credenciais apenas em ambientes controlados. Faça o rotate imediato após validar a migração e configure variáveis protegidas no Netlify/CLI. Evite expor estes tokens em repositórios públicos ou canais inseguros.

- **Business ID:** `persianas2go`
- **Usuário não identificado:** `ZLCWGZWX4CQ1SNC0UPSB3YVKRRM06P`
- **Access Token atual:**
  ```text
eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJUYWNpdXMgQXJpYXMiLCJidXNpbmVzc0lkIjoicGVyc2lhbmFzMmdvIiwibmFtZSI6IlRhY2l1cyBBcmlhcyIsImFwaSI6dHJ1ZSwiaWQiOiJReVVSS0l0UkJHUHRib2FvVGJjZXg0NG9sbTcyIiwiZXhwIjoxOTE2NzY1NTI1LCJqdGkiOiJReVVSS0l0UkJHUHRib2FvVGJjZXg0NG9sbTcyIn0.cBcznxJ2ry8oL4ses0l6eQIbg3ec2nd5k_-ZgYGih7LvVXykcfcRuOWJLJs4B2kXzcVlqMYYku-meGIkcFSurw
  ```
- **Refresh Token:**
  ```text
eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJUYWNpdXMgQXJpYXMiLCJidXNpbmVzc0lkIjoicGVyc2lhbmFzMmdvIiwibmFtZSI6IlRhY2l1cyBBcmlhcyIsImFwaSI6dHJ1ZSwiaWQiOiJReVVSS0l0UkJHUHRib2FvVGJjZXg0NG9sbTcyIiwiZXhwIjoxOTE2NzY1NTI1LCJqdGkiOiJReVVSS0l0UkJHUHRib2FvVGJjZXg0NG9sbTcyIn0.cBcznxJ2ry8oL4ses0l6eQIbg3ec2nd5k_-ZgYGih7LvVXykcfcRuOWJLJs4B2kXzcVlqMYYku-meGIkcFSurw
  ```

## Como configurar no Netlify
1. Abra **Site settings → Build & deploy → Environment**.
2. Adicione as variáveis:
   - `BOTMAKER_API_TOKEN` = valor do *Access Token* acima.
   - `BOTMAKER_BASE_URL` = `https://api.botmaker.com/v2.0` (ou URL customizada, se aplicável).
   - (Opcional) `LOG_DIR` = `/tmp/netlify-logs` para centralizar registros durante execuções serverless.
3. Publique novamente o site para que a função `test_run` consiga autenticar e trazer os dados.

> Após validar, substitua as credenciais por tokens definitivos e remova este arquivo do repositório público.

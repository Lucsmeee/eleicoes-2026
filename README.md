# Painel das eleições 2026

Painel com pesquisas, saldo de votos por estado e região, governadores, Senado, foco no DF e, a partir de 4/10,
a apuração oficial do TSE. Atualiza sozinho e fica sempre no mesmo link:

**https://lucsmeee.github.io/eleicoes-2026/**

Os CSVs para o Power BI ficam no mesmo endereço, por exemplo
`https://lucsmeee.github.io/eleicoes-2026/dim_uf.csv` (Power BI > Obter dados > Web).

## Quando atualiza

| Quando | Horário de Brasília | Por quê |
|---|---|---|
| Todo dia | 07h00 | Pega o que saiu de madrugada e deixa a versão fresca para quem abre de manhã |
| Todo dia | 22h00 | Pega as pesquisas divulgadas à noite (as grandes costumam sair no início da noite) |
| 4/10 e 25/10 | a cada 15 min, 17h–23h45 | Apuração oficial do TSE |
| Quando você edita o snapshot | na hora | Publica as pesquisas novas que você digitou |

O GitHub pode atrasar horários agendados em alguns minutos.

## O que é automático e o que não é

- **Automático:** eleitorado por UF (TSE), tabela de pesquisas nacionais da Wikipédia, apuração do TSE e manchetes do dia (Google Notícias).
- **Manual (snapshot):** pesquisas estaduais, 2º turno por estado, governadores, Senado e DF. Quando sair
  pesquisa nova, edite os blocos no topo do `pipeline_eleicoes_2026.py` pelo próprio site do GitHub
  (ícone de lápis) e salve: o painel republica em 1–2 minutos.

## Notícias do dia

O painel mostra as 4 notícias mais repercutidas sobre Lula e Flávio, com o impacto provável (positivo, negativo,
misto ou neutro). As manchetes vêm do Google Notícias. Para a escolha e a classificação serem automáticas, crie o
secret `ANTHROPIC_API_KEY` (uma chave da API da Anthropic, em console.anthropic.com) em
Settings > Secrets and variables > Actions > Secrets. O custo é de centavos por execução.
Sem a chave, o painel mostra o snapshot manual de notícias (`NOTICIAS_SNAPSHOT` no script) ou, se ele não for do
dia, as 2 primeiras manchetes de cada candidato sem classificação. A classificação automática é uma avaliação de IA:
revise antes de compartilhar.

## Dia da eleição

Se a apuração não aparecer, pegue o código da eleição na URL do site de resultados do TSE e crie a variável
`CODIGO_ELEICAO` em Settings > Secrets and variables > Actions > Variables. No 2º turno, crie também `TURNO` = `2`.

"""
Pipeline ETL – Eleições 2026 (relatório diário)
===============================================
Gera, a cada execução, os CSVs para o Power BI e o painel HTML pronto para compartilhar.

Etapas
  1. EXTRAIR  -> fontes automáticas (cada uma com fallback para o snapshot manual):
                 - Eleitorado por UF: dados abertos do TSE (perfil do eleitorado; baixado 1x e guardado em ./cache)
                 - Pesquisas nacionais de 1º turno: tabela da Wikipédia (en)
                 - Apuração oficial (a partir de 4/10): JSON de resultados do TSE
                 - Notícias do dia: Google Notícias (RSS); se houver ANTHROPIC_API_KEY, o Claude escolhe as 4
                   mais repercutidas e classifica o impacto provável; sem a chave, usa o snapshot do dia
                 O resto (pesquisas estaduais, 2º turno, governadores, Senado, DF) vem do snapshot manual abaixo;
                 atualize esses blocos quando saírem pesquisas novas.
  2. TRATAR   -> normaliza institutos, datas e percentuais.
  3. AGREGAR  -> média, empate técnico, saldo de votos, regiões, situação dos candidatos.
  4. PUBLICAR -> ./saida/*.csv (UTF-8 com BOM, ';', decimal ',') e ./saida/painel_eleicoes_2026.html

Uso diário
  pip install pandas lxml requests
  python pipeline_eleicoes_2026.py                     # tenta tudo; o que falhar usa o snapshot
  python pipeline_eleicoes_2026.py --offline           # só snapshot
  python pipeline_eleicoes_2026.py --codigo-eleicao 619 # força o código da eleição no TSE (dia da apuração)

No GitHub, o workflow .github/workflows/atualizar-painel.yml roda isto sozinho às 7h e às 22h (Brasília)
e publica o painel no GitHub Pages. Veja o README.md.

O arquivo painel_template.html precisa estar na mesma pasta deste script.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import xml.etree.ElementTree as ET
from urllib.parse import quote
import zipfile
from datetime import date, datetime
from zoneinfo import ZoneInfo
from io import StringIO
from pathlib import Path

import pandas as pd

AQUI = Path(__file__).resolve().parent
SAIDA = AQUI / "saida"
CACHE = AQUI / "cache"
TEMPLATE = AQUI / "painel_template.html"

WIKI_URL = "https://en.wikipedia.org/wiki/Opinion_polling_for_the_2026_Brazilian_presidential_election"
TSE_ELEITORADO_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_ATUAL.zip"
TSE_RESULTADOS = "https://resultados.tse.jus.br/oficial"
DATA_ELEICAO = "2026-10-04T08:00:00-03:00"
JANELA_DIAS = 14
MARGEM_PADRAO = 2.0
HEADERS = {"User-Agent": "pipeline-eleicoes/2.0"}

UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE",
       "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]

# ===========================================================================
# SNAPSHOT MANUAL – atualize estes blocos quando saírem pesquisas novas
# ===========================================================================
DATA_SNAPSHOT = "30/09/2026"

SEED_NACIONAL = [
    # instituto, data_fim, lula, flavio, margem, base  (AtlasIntel em votos válidos)
    ("Indexa",              "2026-09-29", 39.0, 34.0, 2.2,  "total"),
    ("Ideia",               "2026-09-28", 39.4, 38.4, 2.2,  "total"),
    ("Vox Brasil",          "2026-09-28", 41.1, 37.8, 2.15, "total"),
    ("AtlasIntel",          "2026-09-28", 46.2, 43.1, 1.0,  "validos"),
    ("Quaest",              "2026-09-27", 39.0, 34.0, 2.0,  "total"),
    ("Nexus/BTG",           "2026-09-27", 42.0, 37.0, 2.0,  "total"),
    ("Datafolha",           "2026-09-23", 40.0, 36.0, 2.0,  "total"),
    ("PoderData",           "2026-09-23", 41.0, 39.0, 1.8,  "total"),
    ("Real Time Big Data",  "2026-09-23", 41.0, 37.0, 2.0,  "total"),
]

# uf, regiao, vencedor_2t_2022 (L/B), lider_1t_2026 (L/F/E), lula_1t, flavio_1t,
# governo_alinhamento (L=aliado Lula, F=aliado Flávio, E=empate técnico, N=nenhum dos dois), lula_2t, flavio_2t
# 1º turno: Quaest 19-23/09 (MA e PI: outros institutos; MS: Quaest de agosto).
# 2º turno: fonte por UF em FONTE_2T_UF.
SEED_UF = [
    ("AC", "Norte", "B", "F", 24, 50, "F", None, None), ("AL", "Nordeste", "L", "L", 44, 32, "E", None, None),
    ("AM", "Norte", "L", "E", 38, 32, "L", None, None), ("AP", "Norte", "B", "L", 42, 35, "F", None, None),
    ("BA", "Nordeste", "L", "L", 58, 23, "E", 64, 28), ("CE", "Nordeste", "L", "L", 55, 23, "E", 63, 28),
    ("DF", "Centro-Oeste", "B", "F", 32, 33, "F", 38, 49), ("ES", "Sudeste", "B", "E", 31, 37, "N", None, None),
    ("GO", "Centro-Oeste", "B", "E", 27, 33, "N", None, None), ("MA", "Nordeste", "L", "L", 59, 22, "N", None, None),
    ("MG", "Sudeste", "L", "E", 35, 30, "F", 40, 40), ("MS", "Centro-Oeste", "B", "F", 27, 33, "F", None, None),
    ("MT", "Centro-Oeste", "B", "F", 25, 50, "F", 30, 56), ("PA", "Norte", "L", "L", None, None, "E", None, None),
    ("PB", "Nordeste", "L", "L", 54, 23, "L", None, None), ("PE", "Nordeste", "L", "L", 54, 21, "N", 58, 25),
    ("PI", "Nordeste", "L", "L", 59, 20, "L", None, None), ("PR", "Sul", "B", "F", 26, 40, "F", 30, 61),
    ("RJ", "Sudeste", "B", "F", 30, 37, "L", 36, 44), ("RN", "Nordeste", "L", "L", 52, 25, "E", None, None),
    ("RO", "Norte", "B", "F", 19, 53, "E", None, None), ("RR", "Norte", "B", "F", 20, 59, "F", None, None),
    ("RS", "Sul", "B", "E", 31, 35, "E", None, None), ("SC", "Sul", "B", "F", 23, 50, "F", 30, 62),
    ("SE", "Nordeste", "L", "L", 54, 26, "L", None, None), ("SP", "Sudeste", "B", "E", 31, 34, "F", 36, 42),
    ("TO", "Norte", "L", "E", 37, 35, "N", None, None),
]
MARGEM_UF = {"SP": 2.0}  # demais estados: 3 p.p.

# Eleitorado apto 2026 – fallback quando o download do TSE falhar (uf: (eleitores, fonte)).
# "oficial" = TSE/TRE; "estimado" = eleitorado 2022 + crescimento, ajustado para fechar o total do TSE.
ELEITORADO = {
    "SP": (34_104_226, "oficial"), "MG": (16_377_659, "oficial"), "RJ": (12_857_000, "oficial"),
    "BA": (11_321_005, "oficial"), "PR": (8_609_026, "oficial"), "RS": (8_526_233, "oficial"),
    "PE": (7_225_744, "oficial"), "CE": (6_998_494, "oficial"), "PA": (6_265_355, "oficial"),
    "SC": (5_725_753, "oficial"), "GO": (5_081_043, "oficial"), "PI": (2_710_000, "oficial"),
    "DF": (2_250_000, "oficial"), "AC": (614_000, "oficial"), "AP": (577_000, "oficial"),
    "RR": (401_000, "oficial"),
    "MA": (5_209_000, "estimado"), "PB": (3_200_000, "estimado"), "ES": (3_034_000, "estimado"),
    "AM": (2_744_000, "estimado"), "MT": (2_733_000, "estimado"), "RN": (2_641_000, "estimado"),
    "AL": (2_423_000, "estimado"), "MS": (2_030_000, "estimado"), "SE": (1_719_000, "estimado"),
    "RO": (1_253_000, "estimado"), "TO": (1_197_000, "estimado"),
}
# Governadores: (uf, candidato, partido, campo, pct) – campo L=aliado Lula, F=aliado Flávio, N=nenhum/indefinido.
# Base: Quaest ago-set (InfoMoney 14/09); SP Datafolha; MG, RJ, CE, SE e DF com rodadas mais recentes; PI outros institutos.
SEED_GOV = [
    ("AC", "Alan Rick", "Republicanos", "F", 33), ("AC", "Mailza Assis", "PP", "F", 24),
    ("AL", "Renan Filho", "MDB", "N", 42), ("AL", "JHC", "PSDB", "N", 40),
    ("AM", "Omar Aziz", "PSD", "L", 26), ("AM", "Roberto Cidade", "União", "N", 18), ("AM", "Maria do Carmo", "PL", "F", 16),
    ("AP", "Dr. Furlan", "PSD", "F", 55), ("AP", "Clécio", "União", "N", 35),
    ("BA", "Jerônimo Rodrigues", "PT", "L", 47), ("BA", "ACM Neto", "União", "N", 43),
    ("CE", "Ciro Gomes", "PSDB", "F", 43), ("CE", "Elmano de Freitas", "PT", "L", 41),
    ("DF", "Celina Leão", "PP", "F", 47), ("DF", "Leandro Grass", "PT", "L", 26), ("DF", "Paula Belmonte", "PSDB", "N", 10), ("DF", "Ricardo Cappelli", "PSB", "L", 4),
    ("ES", "Ricardo Ferraço", "MDB", "N", 35), ("ES", "Lorenzo Pazolini", "Republicanos", "N", 28), ("ES", "Helder Salomão", "PT", "L", 10),
    ("GO", "Daniel Vilela", "MDB", "N", 46), ("GO", "Wilder Morais", "PL", "F", 19), ("GO", "Marconi Perillo", "PSDB", "N", 18),
    ("MA", "Eduardo Braide", "PSD", "N", 44), ("MA", "Orleans Brandão", "MDB", "N", 25), ("MA", "Felipe Camarão", "PT", "L", 6),
    ("MG", "Cleitinho", "Republicanos", "F", 37), ("MG", "Patrus Ananias", "PT", "L", 16),
    ("MS", "Eduardo Riedel", "PP", "F", 40), ("MS", "Fábio Trad", "PT", "L", 13),
    ("MT", "Wellington Fagundes", "PL", "F", 27), ("MT", "Otaviano Pivetta", "Republicanos", "F", 23),
    ("PA", "Dr. Daniel", "Podemos", "N", 28), ("PA", "Hana Ghassan", "MDB", "N", 27),
    ("PB", "Lucas Ribeiro", "PP", "L", 38), ("PB", "Cícero Lucena", "MDB", "N", 19), ("PB", "Efraim Filho", "PL", "F", 18),
    ("PE", "Raquel Lyra", "PSD", "N", 43), ("PE", "João Campos", "PSB", "L", 37),
    ("PI", "Rafael Fonteles", "PT", "L", None), ("PI", "Joel Rodrigues", "PP", "N", None),
    ("PR", "Sergio Moro", "PL", "F", 37), ("PR", "Requião Filho", "PDT", "N", 21),
    ("RJ", "Eduardo Paes", "PSD", "L", 36), ("RJ", "Douglas Ruas", "PL", "F", 23),
    ("RN", "Allyson Bezerra", "União", "N", 25), ("RN", "Cadu de Lula", "PT", "L", 21), ("RN", "Álvaro Dias", "PL", "F", 19),
    ("RO", "Marcos Rogério", "PL", "F", 24), ("RO", "Adailton Fúria", "PSD", "N", 21),
    ("RR", "Arthur Henrique", "PL", "F", 60), ("RR", "Soldado Sampaio", "Republicanos", "N", 27),
    ("RS", "Luciano Zucco", "PL", "F", 26), ("RS", "Juliana Brizola", "PDT", "N", 23),
    ("SC", "Jorginho Mello", "PL", "F", 50), ("SC", "João Rodrigues", "PSD", "N", 17),
    ("SE", "Fábio Mitidieri", "PSD", "L", 43), ("SE", "Valmir de Francisquinho", "Republicanos", "N", 34),
    ("SP", "Tarcísio de Freitas", "Republicanos", "F", 50), ("SP", "Fernando Haddad", "PT", "L", 30),
    ("TO", "Professora Dorinha", "União", "N", 37), ("TO", "Vicentinho Júnior", "PSDB", "N", 28),
]

ELEITORES_EXTERIOR = 918_876     # TSE 2026; votam só para presidente
COMPARECIMENTO = 0.794            # premissa: comparecimento nacional do 2º turno de 2022


# Simulações nacionais de 2º turno (instituto, data_fim, lula, flavio, margem)
SEED_2T_NACIONAL = [
    ("Indexa", "2026-09-29", 43.0, 42.0, 2.2),
    ("Ideia", "2026-09-28", 48.5, 48.0, 2.2),
    ("Vox Brasil", "2026-09-28", 44.7, 45.2, 2.15),
    ("AtlasIntel", "2026-09-28", 50.0, 50.0, 1.0),   # votos válidos
    ("Quaest", "2026-09-27", 42.0, 42.0, 2.0),
    ("Nexus/BTG", "2026-09-27", 46.0, 44.0, 2.0),
    ("PoderData", "2026-09-23", 46.0, 45.0, 1.8),
    ("Real Time Big Data", "2026-09-23", 44.0, 45.0, 2.0),
]

SEED_DF = [
    # cargo, cenario, instituto, candidato, campo, pct
    ("Presidente", "1º turno", "Datafolha", "Flávio Bolsonaro", "Direita", 41),
    ("Presidente", "1º turno", "Datafolha", "Lula", "Esquerda", 34),
    ("Presidente", "1º turno", "Datafolha", "Ronaldo Caiado", "Direita", 9),
    ("Presidente", "2º turno", "Datafolha", "Flávio Bolsonaro", "Direita", 51),
    ("Presidente", "2º turno", "Datafolha", "Lula", "Esquerda", 41),
    ("Presidente", "2º turno", "Quaest", "Flávio Bolsonaro", "Direita", 49),
    ("Presidente", "2º turno", "Quaest", "Lula", "Esquerda", 38),
    ("Governador", "1º turno", "Datafolha", "Celina Leão", "Direita", 42),
    ("Governador", "1º turno", "Datafolha", "Leandro Grass", "Esquerda", 23),
    ("Governador", "1º turno", "Quaest", "Celina Leão", "Direita", 39),
    ("Governador", "1º turno", "Quaest", "Leandro Grass", "Esquerda", 23),
    ("Governador", "1º turno", "Quaest", "Paula Belmonte", "Centro-direita", 9),
    ("Governador", "2º turno", "Quaest", "Celina Leão", "Direita", 54),
    ("Governador", "2º turno", "Quaest", "Leandro Grass", "Esquerda", 31),
    ("Senado", "1º turno", "Datafolha", "Michelle Bolsonaro", "Direita", 22),
    ("Senado", "1º turno", "Datafolha", "Leila do Vôlei", "Centro-esquerda", 19),
    ("Senado", "1º turno", "Datafolha", "Bia Kicis", "Direita", 15),
    ("Senado", "1º turno", "Datafolha", "Erika Kokay", "Esquerda", 15),
    ("Senado", "1º turno", "Quaest", "Michelle Bolsonaro", "Direita", 26),
    ("Senado", "1º turno", "Quaest", "Leila do Vôlei", "Centro-esquerda", 20),
    ("Senado", "1º turno", "Quaest", "Bia Kicis", "Direita", 18),
    ("Senado", "1º turno", "Quaest", "Erika Kokay", "Esquerda", 16),
]

# Fonte do 1º turno por UF (padrão: Quaest 19-23/9)
FONTE_1T_UF = {"MA": "outro instituto", "PI": "outro instituto", "MS": "Quaest, agosto"}
FONTE_1T_PADRAO = "Quaest 19–23/9"
FONTE_2T_UF = {"SP": "Quaest 19–22/9", "RJ": "Quaest 19–22/9", "MG": "Quaest 19–22/9", "DF": "Quaest 19–22/9",
               "MT": "Quaest 21–24/9", "PE": "Quaest 19–22/9", "BA": "Real Time Big Data 23–26/9",
               "CE": "Real Time Big Data 21–24/9", "PR": "Real Time Big Data 11–15/9",
               "SC": "Real Time Big Data 12–16/9"}

NOTAS_UF = {
    "DF": "Datafolha: Flávio 41% × Lula 34% no 1º turno e 51% × 41% no 2º. O TSE barrou Arruda em 23/9.",
    "RJ": "Datafolha dá Lula 38% × 37% no 1º turno.",
    "GO": "Caiado tem 23% no estado.",
    "MT": "2º turno: Quaest 21–24/9.",
}

NOTAS_GOV = {
    "F": "No AC e no MT, os dois primeiros colocados apoiam Flávio.",
    "N": "Em PE, Raquel Lyra (43%) está no limite da margem contra João Campos (37%), candidato oficial de Lula, "
         "e mantém boa relação com o presidente. Em GO, Daniel Vilela tem o apoio de Caiado.",
}

SENADO = {"oposicao": 27, "aliados": 23, "flutuantes": 4, "antes": 34, "depois": 43, "pl": 22, "pt": 9,
          "fonte": "Compilado do Poder360 com as pesquisas estaduais mais recentes (26/9)."}

DF_PAINEL = {
    "pres1": {"fonte": "Datafolha 22–24/9", "itens": [["Flávio", 41, "F"], ["Lula", 34, "L"], ["Caiado", 9, "F"],
                                                       ["Cury", 4, "N"], ["Renan Santos", 3, "N"], ["Zema", 1, "F"]]},
    "pres2": [["Datafolha", 51, 41], ["Quaest", 49, 38], ["2022, resultado real", 58.9, 41.1]],
    "rejeicao": "Rejeição (Correio/Opinião): Lula 51,3%, Flávio 40%.",
    "gov": {"fonte": "Real Time Big Data 24–28/9",
            "itens": [["Celina Leão", 47, "F"], ["Leandro Grass", 26, "L"], ["Paula Belmonte", 10, "N"], ["Cappelli", 4, "L"]],
            "nota": "Em votos válidos, Celina tem 52% e venceria no 1º turno; no 2º, 62% × 38% contra Grass. "
                    "Quaest (25–28/9): Celina 39% × Grass 23% no 1º turno e 54% × 31% no 2º. "
                    "O TSE barrou a candidatura de Arruda em 23/9."},
    "sen": {"fonte": "Real Time Big Data 24–28/9",
            "itens": [["Michelle", 27, "F"], ["Bia Kicis", 18, "F"], ["Leila", 18, "L"], ["Erika Kokay", 16, "L"],
                      ["Sebastião Coelho", 11, "N"]],
            "nota": "Michelle lidera; a segunda vaga tem empate técnico entre Bia Kicis, Leila e Kokay. "
                    "Quaest (25–28/9): Michelle 26%, Leila 20%, Bia Kicis 18%, Kokay 16%."},
}

# Notícias do dia (usado quando não há ANTHROPIC_API_KEY). impacto: positivo | negativo | misto | neutro
NOTICIAS_DATA = "2026-09-30"
NOTICIAS_SNAPSHOT = [
    {"titulo": "Pablo Marçal anuncia apoio a Flávio e aliados especulam cargo ministerial", "fonte": "CNN Brasil",
     "url": "https://www.cnnbrasil.com.br/eleicoes/pablo-marcal-anuncia-apoio-a-flavio-e-aliados-especulam-cargo-ministerial/",
     "candidato": "Flávio", "impacto": "positivo",
     "motivo": "Soma um nome com alcance nas redes a quatro dias da eleição. Ressalva: Marçal divide a direita e o "
               "efeito sobre o voto de quem ainda não decidiu é incerto."},
    {"titulo": "Lula lidera em 1º turno e empata com Flávio no 2º, aponta Indexa/Broadcast", "fonte": "CartaCapital",
     "url": "https://www.cartacapital.com.br/politica/lula-lidera-em-1o-turno-e-empata-com-flavio-bolsonaro-no-2o-aponta-pesquisa-indexa-broadcast/",
     "candidato": "Lula", "impacto": "positivo",
     "motivo": "Cinco pontos à frente no 1º turno (39% × 34%). Ressalva: no 2º turno a diferença é de 1 ponto, "
               "empate técnico."},
    {"titulo": "Indexa/Broadcast: 51% desaprovam governo de Lula; aprovação é de 46%", "fonte": "CNN Brasil",
     "url": "https://www.cnnbrasil.com.br/politica/indexa-broadcast-51-desaprovam-governo-de-lula-aprovacao-e-de-46/",
     "candidato": "Lula", "impacto": "negativo",
     "motivo": "Desaprovação acima da aprovação na reta final pesa contra a reeleição."},
    {"titulo": "Meio/Ideia: Lula tem 48,5% no 2º turno; Flávio, 48%", "fonte": "CNN Brasil",
     "url": "https://www.cnnbrasil.com.br/eleicoes/meio-ideia-lula-tem-485-no-2o-turno-flavio-48/",
     "candidato": "Flávio", "impacto": "misto",
     "motivo": "Empate técnico nos dois turnos (39,4% × 38,4% no 1º), bom para quem vem de trás. Ressalva: Lula "
               "segue numericamente à frente."},
]
MODELO_CLAUDE = "claude-haiku-4-5-20251001"

RODAPE = ("Fontes: Datafolha, Quaest, AtlasIntel, Nexus/BTG, Real Time Big Data, Indexa/Broadcast, Meio/Ideia, Correio Braziliense/Opinião, "
          "InfoMoney, Poder360, TSE e TREs. Classificação esquerda/direita simplificada pelo alinhamento com Lula ou "
          "Flávio. Pesquisas são retratos do momento, não previsões.")


# ===========================================================================
# 1. EXTRAIR
# ===========================================================================
FONTES: list[dict] = []


def registrar(nome: str, ok: bool, status: str) -> None:
    FONTES.append({"nome": nome, "ok": ok, "status": status})
    print(f"[extrair] {nome}: {status}")


def _get(url: str, timeout: int = 60):
    import requests
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r


def extrair_pesquisas(offline: bool) -> pd.DataFrame:
    seed = pd.DataFrame(SEED_NACIONAL, columns=["instituto", "data_fim", "lula", "flavio", "margem", "base"])
    seed["fonte"] = "snapshot"
    if offline:
        registrar("Pesquisas nacionais (Wikipédia)", False, "offline, usando snapshot")
        return seed
    try:
        html = _get(WIKI_URL, 30).text
        for tabela in pd.read_html(StringIO(html), flavor="lxml"):
            if isinstance(tabela.columns, pd.MultiIndex):
                # junta os níveis do cabeçalho ignorando os "Unnamed" e repetições
                nomes = []
                for col in tabela.columns:
                    partes = []
                    for n in map(str, col):
                        if not n.startswith("Unnamed") and n not in partes:
                            partes.append(n)
                    nomes.append(" ".join(partes).strip())
                tabela.columns = nomes
            cols = {c: str(c).lower() for c in tabela.columns}
            col_l = next((c for c, l in cols.items() if "lula" in l), None)
            col_f = next((c for c, l in cols.items() if "bolsonaro" in l), None)
            col_i = next((c for c, l in cols.items() if "pollster" in l or "institut" in l), None)
            col_d = next((c for c, l in cols.items() if any(k in l for k in ("period", "date", "data", "fieldwork"))), None)
            col_m = next((c for c, l in cols.items() if "margin" in l), None)
            col_u = next((c for c, l in cols.items() if "undec" in l or "blank" in l), None)
            if all([col_l, col_f, col_i, col_d]):
                web = tabela[[col_i, col_d, col_l, col_f]].copy()
                web.columns = ["instituto", "data_fim", "lula", "flavio"]
                web["margem"] = tabela[col_m].map(_pct) if col_m else MARGEM_PADRAO
                web["margem"] = web["margem"].fillna(MARGEM_PADRAO)
                # pesquisas com menos de 5% de indecisos/brancos usam método diferente: ficam fora da média
                indec = tabela[col_u].map(_pct) if col_u else None
                web["base"] = "total" if indec is None else indec.map(lambda v: "baixo_indeciso" if v is not None and v < 5 else "total")
                web["fonte"] = "wikipedia"
                registrar("Pesquisas nacionais (Wikipédia)", True, f"{len(web)} linhas lidas")
                return pd.concat([web, seed], ignore_index=True)
        raise ValueError("tabela com Lula e Flávio não encontrada")
    except Exception as erro:
        registrar("Pesquisas nacionais (Wikipédia)", False, f"falhou ({str(erro)[:60]}), usando snapshot")
        return seed


def extrair_eleitorado(offline: bool) -> dict:
    """Soma QT_ELEITORES_PERFIL por UF do perfil do eleitorado do TSE (arquivo grande: baixa 1x e guarda)."""
    base = {uf: ELEITORADO[uf] for uf in UFS}
    cache_csv = CACHE / "eleitorado_uf_tse.csv"
    try:
        if cache_csv.exists():
            tab = pd.read_csv(cache_csv)
        elif offline:
            raise RuntimeError("offline")
        else:
            CACHE.mkdir(exist_ok=True)
            print("[extrair] baixando perfil do eleitorado do TSE (pode demorar alguns minutos)...")
            conteudo = _get(TSE_ELEITORADO_URL, 600).content
            with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
                partes = []
                for nome in [n for n in z.namelist() if n.lower().endswith(".csv")]:
                    with z.open(nome) as f:
                        cab = [c.strip().strip('"').lstrip("\ufeff") for c in
                               pd.read_csv(f, sep=";", encoding="latin-1", nrows=0).columns]
                    col_uf = next((c for c in cab if c == "SG_UF"), None)
                    col_qt = "QT_ELEITORES_PERFIL" if "QT_ELEITORES_PERFIL" in cab else \
                        next((c for c in cab if c.startswith("QT_ELEITORES")), None)
                    if not (col_uf and col_qt):
                        continue
                    with z.open(nome) as f:
                        for bloco in pd.read_csv(f, sep=";", encoding="latin-1", usecols=lambda c: c.strip().strip('"').lstrip("\ufeff") in (col_uf, col_qt),
                                                 chunksize=1_000_000):
                            bloco.columns = [c.strip().strip('"').lstrip("\ufeff") for c in bloco.columns]
                            partes.append(bloco.groupby(col_uf)[col_qt].sum())
                if not partes:
                    raise ValueError(f"nenhum CSV com SG_UF e QT_ELEITORES em {z.namelist()[:5]}")
            tab = pd.concat(partes).groupby(level=0).sum().rename("eleitores").reset_index()
            tab.columns = ["uf", "eleitores"]
            tab.to_csv(cache_csv, index=False)
        oficial = dict(zip(tab["uf"], tab["eleitores"]))
        faltam = [uf for uf in UFS if uf not in oficial]
        if faltam:
            raise ValueError(f"UFs ausentes: {faltam}")
        total = f"{sum(oficial[u] for u in UFS):,}".replace(",", ".")
        registrar("Eleitorado por UF (TSE)", True, f"oficial, {total} eleitores")
        return {uf: (int(oficial[uf]), "oficial") for uf in UFS}
    except Exception as erro:
        motivo = "offline" if str(erro) == "offline" else f"falhou ({str(erro)[:60]})"
        registrar("Eleitorado por UF (TSE)", False, f"{motivo}, usando snapshot (11 UFs estimadas)")
        return base


def _pct_tse(v) -> float:
    return float(str(v).replace(",", ".")) if v not in (None, "") else 0.0


def _descobrir_codigo(turno: int) -> str | None:
    """Procura no config do TSE a eleição geral federal de 2026 (estrutura pode mudar; use --codigo-eleicao se falhar)."""
    cfg = _get(f"{TSE_RESULTADOS}/comum/config/ele-c.json", 30).json()
    achados = []

    def andar(no):
        if isinstance(no, dict):
            texto = " ".join(str(v) for v in no.values() if isinstance(v, str)).lower()
            if "cd" in no and "2026" in texto and "federal" in texto:
                achados.append(no)
            for v in no.values():
                andar(v)
        elif isinstance(no, list):
            for v in no:
                andar(v)

    andar(cfg)
    achados = [a for a in achados if str(a.get("t", turno)) == str(turno)] or achados
    return str(achados[0]["cd"]) if achados else None


def extrair_resultado(offline: bool, codigo: str | None, turno: int) -> dict | None:
    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    if offline or (codigo is None and hoje < date(2026, 10, 4)):
        registrar("Apuração oficial (TSE)", False, "ainda não começou" if not offline else "offline")
        return None
    try:
        codigo = codigo or _descobrir_codigo(turno)
        if not codigo:
            raise ValueError("código da eleição não encontrado; rode com --codigo-eleicao")
        cod6 = f"{int(codigo):06d}"

        def ler(uf: str) -> dict | None:
            url = f"{TSE_RESULTADOS}/ele2026/{int(codigo)}/dados-simplificados/{uf}/{uf}-c0001-e{cod6}-r.json"
            j = _get(url, 30).json()
            cands = j.get("cand", [])
            lula = next((c for c in cands if "LULA" in str(c.get("nm", "")).upper()), None)
            flav = next((c for c in cands if "FL" in str(c.get("nm", "")).upper() and "BOLSONARO" in str(c.get("nm", "")).upper()), None)
            if not (lula and flav):
                return None
            return {"pst": _pct_tse(j.get("pst")), "lula_pct": _pct_tse(lula.get("pvap")),
                    "flavio_pct": _pct_tse(flav.get("pvap")), "lula_votos": int(lula.get("vap", 0) or 0),
                    "flavio_votos": int(flav.get("vap", 0) or 0)}

        br = ler("br")
        if not br:
            raise ValueError("candidatos não encontrados no JSON nacional")
        por_uf = {}
        for uf in UFS:
            try:
                r = ler(uf.lower())
                if r:
                    por_uf[uf] = r
            except Exception:
                pass
        br["por_uf"] = por_uf
        registrar("Apuração oficial (TSE)", True, f"eleição {codigo}, " + f"{br['pst']:.2f}".replace(".", ",") + f"% das seções, {len(por_uf)} UFs")
        return br
    except Exception as erro:
        registrar("Apuração oficial (TSE)", False, f"falhou ({str(erro)[:60]})")
        return None


GNEWS = "https://news.google.com/rss/search?q={q}+when:1d&hl=pt-BR&gl=BR&ceid=BR:pt-419"


def extrair_noticias(offline: bool) -> list[dict]:
    """Manchetes das últimas 24h no Google Notícias, na ordem de relevância do próprio Google."""
    if offline:
        return []
    itens = []
    for cand, termo in [("Lula", "Lula presidente"), ("Flávio", '"Flávio Bolsonaro"')]:
        try:
            raiz = ET.fromstring(_get(GNEWS.format(q=quote(termo)), 30).content)
            for pos, it in enumerate(raiz.iter("item")):
                if pos >= 15:
                    break
                titulo = it.findtext("title", "")
                fonte = it.findtext("source", "") or (titulo.rsplit(" - ", 1)[-1] if " - " in titulo else "")
                if fonte and titulo.endswith(" - " + fonte):
                    titulo = titulo[: -len(" - " + fonte)]
                itens.append({"busca": cand, "posicao": pos + 1, "titulo": titulo.strip(), "fonte": fonte,
                              "url": it.findtext("link", ""), "data": it.findtext("pubDate", "")})
        except Exception as erro:
            print(f"[extrair] Google Notícias ({cand}) falhou: {str(erro)[:60]}")
    return itens


PROMPT_NOTICIAS = """Você recebe manchetes das últimas 24 horas sobre os candidatos à Presidência do Brasil Lula (PT) e \
Flávio Bolsonaro (PL), já na ordem de relevância do Google Notícias.

Tarefa:
1. Agrupe manchetes que tratam do mesmo fato. A repercussão de um fato é maior quanto mais manchetes e veículos \
diferentes o cobrem e quanto mais alto ele aparece na lista.
2. Escolha os 4 fatos mais repercutidos, com pelo menos 1 sobre cada candidato. Ignore colunas de opinião e \
agendas sem fato novo, a menos que não haja outra opção.
3. Para cada fato, classifique o impacto eleitoral PROVÁVEL sobre o candidato principal da notícia: \
"positivo", "negativo", "misto" ou "neutro". Avalie o efeito provável sobre a percepção do eleitor médio, não o \
mérito da medida nem a sua opinião. Use exatamente o mesmo critério para os dois candidatos. Na dúvida, use \
"misto" ou "neutro".
4. Escreva um motivo em até 2 frases, factual, citando a principal ressalva.

Responda APENAS com JSON, sem texto antes ou depois, no formato:
[{"titulo": "...", "fonte": "...", "url": "...", "candidato": "Lula" | "Flávio", "impacto": "...", "motivo": "..."}]
Use o título, a fonte e a url da manchete mais representativa de cada fato.

Manchetes:
"""


def classificar_noticias(itens: list[dict]) -> tuple[list[dict], str]:
    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d")
    chave = os.environ.get("ANTHROPIC_API_KEY")
    if itens and chave:
        try:
            import requests
            lista = "\n".join(f'{i["busca"]} #{i["posicao"]} | {i["titulo"]} | {i["fonte"]} | {i["url"]}' for i in itens)
            r = requests.post("https://api.anthropic.com/v1/messages", timeout=90, headers={
                "x-api-key": chave, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": MODELO_CLAUDE, "max_tokens": 1500,
                      "messages": [{"role": "user", "content": PROMPT_NOTICIAS + lista}]})
            r.raise_for_status()
            texto = "".join(b.get("text", "") for b in r.json()["content"])
            texto = re.sub(r"```(json)?", "", texto).strip()
            noticias = json.loads(texto)[:4]
            validos = {"positivo", "negativo", "misto", "neutro"}
            for n in noticias:
                n["impacto"] = n.get("impacto", "neutro") if n.get("impacto") in validos else "neutro"
            registrar("Notícias do dia (Google Notícias + Claude)", True, f"{len(itens)} manchetes, {len(noticias)} classificadas")
            return noticias, "automática (Claude), revise antes de compartilhar"
        except Exception as erro:
            registrar("Notícias do dia (Google Notícias + Claude)", False, f"classificação falhou ({str(erro)[:50]})")
    if NOTICIAS_DATA == hoje:
        registrar("Notícias do dia", False, "snapshot manual de hoje")
        return NOTICIAS_SNAPSHOT, "editorial (snapshot manual)"
    if itens:
        top = [i for c in ("Lula", "Flávio") for i in [x for x in itens if x["busca"] == c][:2]]
        registrar("Notícias do dia (Google Notícias)", True, "manchetes sem classificação (falta ANTHROPIC_API_KEY)")
        return [{**i, "candidato": i["busca"], "impacto": "sem classificação", "motivo": ""} for i in top], "sem classificação"
    data_snap = datetime.strptime(NOTICIAS_DATA, "%Y-%m-%d").strftime("%d/%m")
    registrar("Notícias do dia", False, f"sem acesso; mostrando snapshot de {data_snap}")
    return NOTICIAS_SNAPSHOT, f"snapshot de {data_snap}"


# ===========================================================================
# 2. TRATAR
# ===========================================================================
def _pct(valor) -> float | None:
    if pd.isna(valor):
        return None
    achado = re.search(r"\d+(?:[.,]\d+)?", str(valor))
    return float(achado.group().replace(",", ".")) if achado else None


def _data(valor) -> pd.Timestamp:
    texto = str(valor)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto):
        return pd.to_datetime(texto, format="%Y-%m-%d")
    texto = re.split(r"\s*[–-]\s*", texto)[-1].strip()      # "30 Aug – 2 Sep" -> "2 Sep"
    if not re.search(r"\d{4}", texto):
        texto += " 2026"
    return pd.to_datetime(texto, errors="coerce", dayfirst=True)


def tratar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["instituto"] = (df["instituto"].astype(str).str.replace(r"\[.*?\]", "", regex=True)
                       .str.split("/").str[0].str.strip())
    df["instituto"] = df["instituto"].replace({"Real Time": "Real Time Big Data", "Genial": "Quaest", "Meio": "Ideia"})
    df["data_fim"] = df["data_fim"].map(_data)
    for col in ["lula", "flavio", "margem"]:
        df[col] = df[col].map(_pct)
    df = df.dropna(subset=["data_fim", "lula", "flavio"])
    df = df[(df["lula"].between(1, 100)) & (df["flavio"].between(1, 100))]
    df = (df.sort_values("fonte", key=lambda s: s.eq("snapshot"), ascending=False)
            .drop_duplicates(subset=["instituto", "data_fim"]))
    return df.sort_values("data_fim", ascending=False).reset_index(drop=True)


# ===========================================================================
# 3. AGREGAR
# ===========================================================================
def agregar(df: pd.DataFrame, eleitorado: dict):
    df = df.copy()
    df["diferenca"] = (df["lula"] - df["flavio"]).round(1)
    df["empate_tecnico"] = df["diferenca"].abs() <= 2 * df["margem"]
    corte = df["data_fim"].max() - pd.Timedelta(days=JANELA_DIAS)
    recentes = df[(df["data_fim"] >= corte) & (df["base"] == "total")].drop_duplicates("instituto")
    media = {"lula": round(recentes["lula"].mean(), 1), "flavio": round(recentes["flavio"].mean(), 1),
             "n": int(len(recentes)), "janela": JANELA_DIAS}

    uf = pd.DataFrame(SEED_UF, columns=["uf", "regiao", "vencedor_2022", "lider_2026", "lula_1t_2026",
                                        "flavio_1t_2026", "governo_alinhamento", "lula_2t_2026", "flavio_2t_2026"])

    def lider_2t(r):
        if pd.isna(r["lula_2t_2026"]):
            return "?"
        dif = r["lula_2t_2026"] - r["flavio_2t_2026"]
        if abs(dif) <= 2 * MARGEM_UF.get(r["uf"], 3.0):
            return "E"
        return "L" if dif > 0 else "F"

    uf["lider_2t_2026"] = uf.apply(lider_2t, axis=1)
    total = sum(eleitorado[u][0] for u in UFS) + ELEITORES_EXTERIOR
    uf["eleitorado"] = uf["uf"].map(lambda u: eleitorado[u][0])
    uf["eleitorado_fonte"] = uf["uf"].map(lambda u: eleitorado[u][1])
    uf["peso_nacional_pct"] = (uf["eleitorado"] / total * 100).round(2)
    uf["votos_estimados"] = (uf["eleitorado"] * COMPARECIMENTO).round()
    uf["saldo_1t"] = (uf["votos_estimados"] * (uf["lula_1t_2026"] - uf["flavio_1t_2026"]) / 100).round()
    uf["saldo_2t"] = (uf["votos_estimados"] * (uf["lula_2t_2026"] - uf["flavio_2t_2026"]) / 100).round()

    gov = pd.DataFrame(SEED_GOV, columns=["uf", "candidato", "partido", "campo", "pct"])
    gov["posicao"] = gov.groupby("uf").cumcount() + 1
    lider = gov[gov["posicao"] == 1].set_index("uf")["pct"]
    vice = gov[gov["posicao"] == 2].set_index("uf")["pct"]
    dif = lider - vice

    def situacao(r):
        d = dif.get(r["uf"])
        if pd.isna(d):
            return "Favorito (outros institutos)" if r["posicao"] == 1 else "Atrás"
        if r["posicao"] == 1:
            return "Favorito" if d > 6 else "Empate técnico"
        return "Empate técnico" if lider[r["uf"]] - r["pct"] <= 6 else "Atrás"

    gov["situacao"] = gov.apply(situacao, axis=1)

    u2 = uf.assign(votos_lula=uf["votos_estimados"] * uf["lula_1t_2026"] / 100,
                   votos_flavio=uf["votos_estimados"] * uf["flavio_1t_2026"] / 100,
                   sem_pesquisa=uf["votos_estimados"].where(uf["lula_1t_2026"].isna(), 0))
    reg = u2.groupby("regiao").agg(eleitorado=("eleitorado", "sum"), votos_estimados=("votos_estimados", "sum"),
                                   votos_lula=("votos_lula", "sum"), votos_flavio=("votos_flavio", "sum"),
                                   votos_sem_pesquisa=("sem_pesquisa", "sum")).round().reset_index()
    reg["saldo"] = reg["votos_lula"] - reg["votos_flavio"]
    seg = pd.DataFrame(SEED_2T_NACIONAL, columns=["instituto", "data_fim", "lula", "flavio", "margem"])
    seg["diferenca"] = (seg["lula"] - seg["flavio"]).round(1)
    seg["empate_tecnico"] = seg["diferenca"].abs() <= 2 * seg["margem"]
    return df, uf, gov, reg, seg, media


# ===========================================================================
# 4. PUBLICAR
# ===========================================================================
def _num(v):
    return None if pd.isna(v) else (int(v) if float(v).is_integer() else float(v))


def publicar(pesq, uf, gov, reg, seg, media, resultado, atualizado: str, noticias=None, criterio="") -> Path:
    SAIDA.mkdir(exist_ok=True)
    op = dict(index=False, sep=";", decimal=",", encoding="utf-8-sig")
    pesq.to_csv(SAIDA / "fato_pesquisas_nacional.csv", date_format="%Y-%m-%d", **op)
    uf.to_csv(SAIDA / "dim_uf.csv", **op)
    gov.to_csv(SAIDA / "fato_governadores.csv", **op)
    reg.to_csv(SAIDA / "fato_regiao.csv", **op)
    seg.to_csv(SAIDA / "fato_segundo_turno_nacional.csv", **op)
    pd.DataFrame(SEED_DF, columns=["cargo", "cenario", "instituto", "candidato", "campo", "pct"]).to_csv(SAIDA / "fato_df.csv", **op)
    pd.DataFrame(noticias or []).to_csv(SAIDA / "fato_noticias.csv", **op)
    if resultado:
        linhas = [{"uf": "BR", **{k: v for k, v in resultado.items() if k != "por_uf"}}]
        linhas += [{"uf": k, **v} for k, v in resultado["por_uf"].items()]
        pd.DataFrame(linhas).to_csv(SAIDA / "fato_apuracao_tse.csv", **op)

    dia = lambda d: f"{d.day}/{d.month}"
    dados = {
        "atualizado": atualizado, "eleicao": DATA_ELEICAO, "comparecimento": COMPARECIMENTO, "media": media,
        "pesquisas": [{"i": r.instituto, "d": dia(r.data_fim), "l": r.lula, "f": r.flavio, "base": r.base}
                      for r in pesq.drop_duplicates("instituto").head(10).itertuples()],
        "seg2t": [{"i": r.instituto, "d": dia(pd.to_datetime(r.data_fim)), "l": r.lula, "f": r.flavio,
                   "base": "validos" if r.instituto == "AtlasIntel" else "total"} for r in seg.itertuples()],
        "ufs": [{"uf": r.uf, "regiao": r.regiao, "v22": r.vencedor_2022, "l26": r.lider_2026, "gov": r.governo_alinhamento,
                 "l1": _num(r.lula_1t_2026), "f1": _num(r.flavio_1t_2026), "fonte1": FONTE_1T_UF.get(r.uf, FONTE_1T_PADRAO),
                 "l2": _num(r.lula_2t_2026), "f2": _num(r.flavio_2t_2026), "lider2": r.lider_2t_2026, "fonte2": FONTE_2T_UF.get(r.uf),
                 "eleit": int(r.eleitorado), "peso": float(r.peso_nacional_pct), "fonte_eleit": r.eleitorado_fonte,
                 "saldo1": _num(r.saldo_1t), "saldo2": _num(r.saldo_2t), "nota": NOTAS_UF.get(r.uf)}
                for r in uf.itertuples()],
        "gov": [[r.uf, r.candidato, r.partido, r.campo, _num(r.pct), r.situacao] for r in gov.itertuples()],
        "reg": [[r.regiao, int(r.votos_estimados), int(r.votos_lula), int(r.votos_flavio), int(r.votos_sem_pesquisa)]
                for r in reg.itertuples()],
        "noticias": noticias or [], "criterio_noticias": criterio, "notas_gov": NOTAS_GOV, "senado": SENADO, "df": DF_PAINEL, "resultado": resultado,
        "fontes": FONTES + [{"nome": "Pesquisas estaduais, 2º turno, governadores, Senado e DF", "ok": False,
                             "status": f"snapshot manual de {DATA_SNAPSHOT}"}],
        "rodape": RODAPE,
    }
    html = TEMPLATE.read_text(encoding="utf-8").replace("/*__DADOS__*/null", json.dumps(dados, ensure_ascii=False))
    destino = SAIDA / "painel_eleicoes_2026.html"
    destino.write_text(html, encoding="utf-8")

    com = uf.dropna(subset=["saldo_1t"])
    print(f"\n=== ELEIÇÕES 2026 – {atualizado} ===")
    print(f"Média: Lula {media['lula']}% x Flávio {media['flavio']}% ({media['n']} pesquisas)")
    print(f"Saldo líquido estimado: {com['saldo_1t'].sum()/1e6:+.2f} mi (positivo = Lula)")
    if resultado:
        print(f"APURAÇÃO TSE: {resultado['pst']:.2f}% das seções | Lula {resultado['lula_pct']}% x Flávio {resultado['flavio_pct']}%")
    print(f"Painel: {destino}")
    return destino


def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline diário das eleições 2026")
    ap.add_argument("--offline", action="store_true", help="não acessa a internet; usa só o snapshot")
    ap.add_argument("--codigo-eleicao", help="código da eleição no TSE (ex.: 619), se a descoberta automática falhar")
    ap.add_argument("--turno", type=int, default=1, choices=[1, 2])
    args = ap.parse_args()

    eleitorado = extrair_eleitorado(args.offline)
    bruto = extrair_pesquisas(args.offline)
    resultado = extrair_resultado(args.offline, args.codigo_eleicao, args.turno)
    noticias, criterio = classificar_noticias(extrair_noticias(args.offline))
    pesq, uf, gov, reg, seg, media = agregar(tratar(bruto), eleitorado)
    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    publicar(pesq, uf, gov, reg, seg, media, resultado, agora.strftime("%d/%m/%Y às %Hh%M"), noticias, criterio)


if __name__ == "__main__":
    main()

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
DATA_SNAPSHOT = "03/10/2026"

SEED_NACIONAL = [
    # instituto, data_fim, lula, flavio, margem, base  (AtlasIntel quase sem indecisos: fica fora da média)
    ("PoderData",           "2026-10-02", 42.0, 41.0, 1.5,  "total"),
    ("CNT/MDA",             "2026-10-02", 43.1, 38.0, 2.2,  "total"),
    ("Datafolha",           "2026-10-01", 42.0, 38.0, 2.0,  "total"),
    ("Real Time Big Data",  "2026-09-30", 43.0, 39.0, 2.0,  "total"),
    ("Indexa",              "2026-09-29", 39.0, 34.0, 2.2,  "total"),
    ("Vox Brasil",          "2026-09-28", 41.1, 37.8, 2.15, "total"),
    ("Ideia",               "2026-09-28", 39.4, 38.4, 2.2,  "total"),
    ("AtlasIntel",          "2026-09-28", 45.3, 42.2, 1.0,  "baixo_indeciso"),
    ("Quaest",              "2026-09-27", 39.0, 34.0, 2.0,  "total"),
    ("Nexus/BTG",           "2026-09-27", 42.0, 37.0, 2.0,  "total"),
    ("Datafolha",           "2026-09-23", 40.0, 36.0, 2.0,  "total"),
]

# uf, regiao, vencedor_2t_2022 (L/B), lider_1t_2026 (L/F/E), lula_1t, flavio_1t,
# governo_alinhamento (L=aliado Lula, F=aliado Flávio, E=empate técnico, N=nenhum dos dois), lula_2t, flavio_2t
# 1º turno: Quaest 19-23/09, exceto SP, RJ, MG, DF e PE (Quaest 25-28/09) e PA (Quaest 22-25/09); MA e PI: outros
# institutos; MS: Quaest de agosto.
# 2º turno: fonte por UF em FONTE_2T_UF.
SEED_UF = [
    ("AC", "Norte", "B", "F", 24, 50, "F", 25, 59), ("AL", "Nordeste", "L", "L", 44, 32, "E", 53, 38),
    ("AM", "Norte", "L", "E", 38, 32, "L", 45, 40), ("AP", "Norte", "B", "L", 42, 35, "F", 41, 45),
    ("BA", "Nordeste", "L", "L", 58, 23, "E", 64, 28), ("CE", "Nordeste", "L", "L", 55, 23, "E", 63, 28),
    ("DF", "Centro-Oeste", "B", "F", 32, 38, "F", 33, 48), ("ES", "Sudeste", "B", "E", 31, 37, "N", 41, 48),
    ("GO", "Centro-Oeste", "B", "E", 27, 33, "N", 33, 57), ("MA", "Nordeste", "L", "L", 59, 22, "N", 63, 26),
    ("MG", "Sudeste", "L", "E", 35, 31, "F", 41, 40), ("MS", "Centro-Oeste", "B", "F", 27, 33, "F", 38, 52),
    ("MT", "Centro-Oeste", "B", "F", 25, 50, "F", 30, 56), ("PA", "Norte", "L", "L", 40, 35, "E", 44, 38),
    ("PB", "Nordeste", "L", "L", 54, 23, "L", 60, 26), ("PE", "Nordeste", "L", "L", 58, 20, "N", 61, 24),
    ("PI", "Nordeste", "L", "L", 59, 20, "L", 68.3, 24.4), ("PR", "Sul", "B", "F", 26, 40, "F", 30, 61),
    ("RJ", "Sudeste", "B", "F", 31, 38, "L", 34, 44), ("RN", "Nordeste", "L", "L", 52, 25, "E", 56, 30),
    ("RO", "Norte", "B", "F", 19, 53, "E", 27.3, 68.8), ("RR", "Norte", "B", "F", 20, 59, "F", 22, 67),
    ("RS", "Sul", "B", "E", 31, 35, "E", 43, 53), ("SC", "Sul", "B", "F", 23, 50, "F", 30, 62),
    ("SE", "Nordeste", "L", "L", 54, 26, "L", 62, 28), ("SP", "Sudeste", "B", "F", 30, 36, "F", 36, 42),
    ("TO", "Norte", "L", "E", 37, 35, "N", 40, 48),
]
# Margem de erro da pesquisa de 2º turno usada em cada UF (empate técnico = diferença até 2× a margem).
# Real Time Big Data e AtlasIntel estaduais: 2 p.p.; Quaest (exceto SP) e Veritá: 3 p.p. (padrão).
MARGEM_UF = {"SP": 2.0, "BA": 2.0, "CE": 2.0, "PR": 2.0, "RS": 2.0, "PA": 2.0, "AL": 2.0, "ES": 2.0, "TO": 2.0,
             "GO": 2.0, "SE": 2.0, "SC": 2.0, "MS": 2.0, "AP": 2.0, "PI": 2.0}

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
    ("CNT/MDA", "2026-10-02", 47.3, 43.1, 2.2),
    ("Datafolha", "2026-10-01", 48.0, 45.0, 2.0),
    ("Real Time Big Data", "2026-09-30", 45.0, 46.0, 2.0),
    ("Indexa", "2026-09-29", 43.0, 42.0, 2.2),
    ("Vox Brasil", "2026-09-28", 44.7, 45.2, 2.15),
    ("Ideia", "2026-09-28", 48.5, 48.0, 2.2),
    ("AtlasIntel", "2026-09-28", 47.6, 47.7, 1.0),
    ("Quaest", "2026-09-27", 42.0, 42.0, 2.0),
    ("Nexus/BTG", "2026-09-27", 46.0, 44.0, 2.0),
    ("Datafolha", "2026-09-24", 47.0, 45.0, 2.0),
    ("PoderData", "2026-09-23", 46.0, 45.0, 1.8),
]

SEED_DF = [
    # cargo, cenario, instituto, candidato, campo, pct
    ("Presidente", "1º turno", "Datafolha", "Flávio Bolsonaro", "Direita", 41),
    ("Presidente", "1º turno", "Datafolha", "Lula", "Esquerda", 34),
    ("Presidente", "1º turno", "Datafolha", "Ronaldo Caiado", "Direita", 9),
    ("Presidente", "2º turno", "Datafolha", "Flávio Bolsonaro", "Direita", 51),
    ("Presidente", "2º turno", "Datafolha", "Lula", "Esquerda", 41),
    ("Presidente", "1º turno", "Quaest", "Flávio Bolsonaro", "Direita", 38),
    ("Presidente", "1º turno", "Quaest", "Lula", "Esquerda", 32),
    ("Presidente", "2º turno", "Quaest", "Flávio Bolsonaro", "Direita", 48),
    ("Presidente", "2º turno", "Quaest", "Lula", "Esquerda", 33),
    ("Governador", "1º turno", "Quaest", "Celina Leão", "Direita", 39),
    ("Governador", "1º turno", "Quaest", "Leandro Grass", "Esquerda", 23),
    ("Governador", "1º turno", "Quaest", "Paula Belmonte", "Centro-direita", 9),
    ("Governador", "2º turno", "Quaest", "Celina Leão", "Direita", 54),
    ("Governador", "2º turno", "Quaest", "Leandro Grass", "Esquerda", 31),
    ("Governador", "1º turno", "Datafolha", "Celina Leão", "Direita", 46),
    ("Governador", "1º turno", "Datafolha", "Leandro Grass", "Esquerda", 22),
    ("Governador", "1º turno", "Datafolha", "Paula Belmonte", "Centro-direita", 9),
    ("Governador", "2º turno", "Datafolha", "Celina Leão", "Direita", 55),
    ("Governador", "2º turno", "Datafolha", "Leandro Grass", "Esquerda", 32),
    ("Senado", "1º turno", "Datafolha", "Michelle Bolsonaro", "Direita", 23),
    ("Senado", "1º turno", "Datafolha", "Bia Kicis", "Direita", 18),
    ("Senado", "1º turno", "Datafolha", "Leila do Vôlei", "Centro-esquerda", 18),
    ("Senado", "1º turno", "Datafolha", "Erika Kokay", "Esquerda", 13),
    ("Senado", "1º turno", "Quaest", "Michelle Bolsonaro", "Direita", 26),
    ("Senado", "1º turno", "Quaest", "Leila do Vôlei", "Centro-esquerda", 20),
    ("Senado", "1º turno", "Quaest", "Bia Kicis", "Direita", 18),
    ("Senado", "1º turno", "Quaest", "Erika Kokay", "Esquerda", 16),
]

# Fonte do 1º turno por UF (padrão: Quaest 19-23/9)
FONTE_1T_UF = {"MA": "outro instituto", "PI": "outro instituto", "MS": "Quaest, agosto",
               "SP": "Quaest 25–28/9", "RJ": "Quaest 25–28/9", "MG": "Quaest 25–28/9", "DF": "Quaest 25–28/9",
               "PE": "Quaest 25–28/9", "PA": "Quaest 22–25/9"}
FONTE_1T_PADRAO = "Quaest 19–23/9"
# 2º turno: a pesquisa estadual mais recente de Lula × Flávio em cada UF
FONTE_2T_UF = {"SP": "Quaest 19–22/9", "RJ": "Quaest 25–28/9", "MG": "Quaest 25–28/9", "DF": "Quaest 25–28/9",
               "PE": "Quaest 25–28/9", "MT": "Quaest 21–24/9", "MA": "Quaest 21–24/9", "RN": "Quaest 21–24/9",
               "AM": "Quaest 20–23/9", "PB": "Quaest 18–21/9", "AC": "Quaest, setembro", "RR": "Quaest, setembro",
               "BA": "Real Time Big Data 23–26/9", "CE": "Real Time Big Data 21–24/9",
               "PR": "Real Time Big Data 24–28/9", "RS": "Real Time Big Data 24–28/9",
               "PA": "Real Time Big Data 23–26/9", "AL": "Real Time Big Data 21–24/9",
               "ES": "Real Time Big Data 21–24/9", "TO": "Real Time Big Data 21–24/9",
               "GO": "Real Time Big Data 17–21/9", "SE": "Real Time Big Data 17–21/9",
               "SC": "Real Time Big Data 12–16/9", "MS": "Real Time Big Data 5–9/9",
               "AP": "Real Time Big Data 3–7/9", "RO": "Veritá 21–25/9", "PI": "AtlasIntel 28/8–2/9"}

NOTAS_UF = {
    "DF": "Quaest 25–28/9: Flávio 38% × Lula 32% no 1º turno e 48% × 33% no 2º. Datafolha 22–24/9: 41% × 34% e "
          "51% × 41%. O TSE barrou Arruda em 23/9.",
    "RJ": "Datafolha dá Lula 38% × 37% no 1º turno.",
    "GO": "Caiado tem 23% no estado.",
    "MT": "2º turno: Quaest 21–24/9.",
    "TO": "Quaest 19–22/9 dá empate em 44% no 2º turno; a Real Time Big Data, mais recente, Flávio 48% × 40%.",
    "PA": "Real Time Big Data 23–26/9 dá Lula 45% × 36% no 1º turno.",
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
    "pres2": [["Datafolha 22–24/9", 51, 41], ["Quaest 25–28/9", 48, 33], ["2022, resultado real", 58.9, 41.1]],
    "rejeicao": "Rejeição (Correio/Opinião): Lula 51,3%, Flávio 40%.",
    "gov": {"fonte": "Datafolha 28/9–1/10",
            "itens": [["Celina Leão", 46, "F"], ["Leandro Grass", 22, "L"], ["Paula Belmonte", 9, "N"]],
            "nota": "Em votos válidos, Celina tem 53% e venceria no 1º turno; no 2º, 55% × 32% contra Grass. "
                    "O TSE barrou a candidatura de Arruda em 23/9."},
    "sen": {"fonte": "Datafolha 28/9–1/10",
            "itens": [["Michelle", 23, "F"], ["Bia Kicis", 18, "F"], ["Leila", 18, "L"], ["Erika Kokay", 13, "L"]],
            "nota": "Michelle lidera; Bia Kicis e Leila empatam na disputa pela segunda vaga."},
}

# Notícias do dia (usado quando não há ANTHROPIC_API_KEY). impacto: positivo | negativo | misto | neutro
NOTICIAS_DATA = "2026-10-03"
NOTICIAS_SNAPSHOT = [
    {"titulo": "CNT/MDA na véspera: Lula tem 43,1% e Flávio 38%; 47,3% × 43,1% no 2º turno", "fonte": "Gazeta do Povo",
     "url": "https://www.gazetadopovo.com.br/eleicoes/2026/pesquisa-eleitoral-2026/cnt-mda-presidente-outubro-2026-vespera-primeiro-turno/",
     "candidato": "Lula", "impacto": "positivo",
     "motivo": "Lidera o 1º turno fora da margem (47,8% dos válidos) na pesquisa que mais acertou em 2022. "
               "Ressalva: no 2º turno Flávio subiu de 40% para 43,1% desde setembro e a diferença ficou no limite da margem."},
    {"titulo": "Lula no Flow passa de 10 milhões de acessos em 12 horas", "fonte": "Revista Fórum",
     "url": "https://revistaforum.com.br/politica/lula-no-flow-podcast-ultrapassa-10-milhoes-de-acessos/",
     "candidato": "Lula", "impacto": "positivo",
     "motivo": "Ocupou o horário do debate cancelado com audiência recorde e falou com um público jovem que não é o seu. "
               "Ressalva: audiência não é voto, e a troca do debate pelo podcast segue criticada pelos adversários."},
    {"titulo": "Flávio faz giro por MG, SP e RJ com Nikolas e Tarcísio nas últimas 48 horas", "fonte": "Brasil em Folhas",
     "url": "https://www.brasilemfolhas.com.br/2026/10/flavio-bolsonaro-faz-giro-por-tres-estados-na-reta-final-da-campanha/",
     "candidato": "Flávio", "impacto": "positivo",
     "motivo": "Fecha a campanha nos três maiores colégios, que somam mais de um terço do eleitorado, ao lado de aliados "
               "com voto próprio. Ressalva: atos de véspera costumam mudar pouco o resultado."},
    {"titulo": "As estratégias de Flávio e Lula no último dia antes do 1º turno", "fonte": "Metrópoles",
     "url": "https://www.metropoles.com/brasil/as-estrategias-de-flavio-bolsonaro-e-lula-no-ultimo-dia-antes-do-1o-turno",
     "candidato": "Flávio", "impacto": "neutro",
     "motivo": "Os dois encerram em São Paulo, onde o Datafolha dá empate técnico; Flávio busca o voto útil da direita e "
               "Lula mobiliza eleitores com 70 anos ou mais. Ressalva: o efeito depende do comparecimento de domingo."},
]
MODELO_CLAUDE = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# MODO PÚBLICO (Res. TSE 23.600/2019, art. 10): toda pesquisa divulgada precisa mostrar período, margem de erro,
# nível de confiança, nº de entrevistas, quem fez, quem contratou e o nº de registro no TSE.
# Com PUBLICO = True, o painel só mostra (e só usa na média) pesquisas que têm ficha completa abaixo, e esconde
# as seções baseadas em pesquisas estaduais até que elas também tenham ficha.
# ---------------------------------------------------------------------------
PUBLICO = False
NOME_SAIDA = "painel_eleicoes_2026.html"

# (instituto, data_fim) -> ficha. Fonte das fichas: matérias que reproduzem o registro (Gazeta do Povo, TVT, GMC).
FICHAS = {
    ("PoderData", "2026-10-02"): {"registro": "BR-03519/2026", "periodo": "30/9 a 2/10/2026", "entrevistas": 4000,
                                  "margem": "±1,5 p.p.", "confianca": "95%", "realizou": "PoderData",
                                  "contratou": "Poder360 (recursos próprios)"},
    ("CNT/MDA", "2026-10-02"): {"registro": "BR-04756/2026", "periodo": "30/9 a 2/10/2026", "entrevistas": 2007,
                                "margem": "±2,2 p.p.", "confianca": "95%", "realizou": "Instituto MDA",
                                "contratou": "Confederação Nacional do Transporte (CNT)"},
    ("Datafolha", "2026-10-01"): {"registro": "BR-08039/2026", "periodo": "28/9 a 1º/10/2026", "entrevistas": 2506,
                                  "margem": "±2 p.p.", "confianca": "95%", "realizou": "Datafolha",
                                  "contratou": "Folha de S.Paulo e Grupo Globo"},
    ("Datafolha", "2026-09-24"): {"registro": "BR-00304/2026", "periodo": "22 a 24/9/2026", "entrevistas": 2002,
                                  "margem": "±2 p.p.", "confianca": "95%", "realizou": "Datafolha",
                                  "contratou": "Folha de S.Paulo e Grupo Globo"},
    ("Datafolha", "2026-09-16"): {"registro": "BR-04029/2026", "periodo": "15 a 16/9/2026", "entrevistas": 2002,
                                  "margem": "±2 p.p.", "confianca": "95%", "realizou": "Datafolha",
                                  "contratou": "Empresa Folha da Manhã S.A. e Globo Comunicação e Participações S.A."},
    ("Datafolha", "2026-09-10"): {"registro": "BR-01833/2026", "periodo": "8 a 10/9/2026", "entrevistas": 2002,
                                  "margem": "±2 p.p.", "confianca": "95%", "realizou": "Datafolha",
                                  "contratou": "Empresa Folha da Manhã S.A. e Globo Comunicação e Participações S.A."},
    ("Quaest", "2026-09-27"): {"registro": "BR-06520/2026", "periodo": "24 a 27/9/2026", "entrevistas": 2004,
                               "margem": "±2 p.p.", "confianca": "95%", "realizou": "Quaest",
                               "contratou": "TV Globo e O Globo"},
    ("AtlasIntel", "2026-09-28"): {"registro": "BR-04391/2026", "periodo": "23 a 28/9/2026", "entrevistas": 5005,
                                   "margem": "±1 p.p.", "confianca": "95%", "realizou": "AtlasIntel",
                                   "contratou": "recursos próprios (divulgação em parceria com a Bloomberg)"},
}

# 1º turno, votos totais, todos os candidatos que pontuaram: (instituto, data_fim) -> {candidato: %}
CANDIDATOS_1T = {
    ("Datafolha", "2026-10-01"): {"Lula (PT)": 42, "Flávio Bolsonaro (PL)": 38, "Augusto Cury (Avante)": 4,
                                  "Ronaldo Caiado (PSD)": 3, "Renan Santos (Missão)": 3, "Romeu Zema (Novo)": 1,
                                  "Samara Martins (UP)": 1, "Wilson Grassi (Democrata)": 1,
                                  "Brancos e nulos": 5, "Indecisos": 2},
    ("Quaest", "2026-09-27"): {"Lula (PT)": 39, "Flávio Bolsonaro (PL)": 34, "Augusto Cury (Avante)": 4,
                               "Ronaldo Caiado (PSD)": 4, "Renan Santos (Missão)": 3, "Romeu Zema (Novo)": 1},
    ("AtlasIntel", "2026-09-28"): {"Lula (PT)": 45.3, "Flávio Bolsonaro (PL)": 42.2, "Renan Santos (Missão)": 5.2,
                                   "Augusto Cury (Avante)": 2.0, "Ronaldo Caiado (PSD)": 1.8, "Romeu Zema (Novo)": 0.9,
                                   "Samara Martins (UP)": 0.4, "Rui Costa Pimenta (PCO)": 0.1,
                                   "Brancos e nulos": 0.9, "Indecisos": 1.2},
}

# Cenários de 2º turno com outros adversários: (instituto, data_fim, adversário, % Lula, % adversário)
CENARIOS_2T = [
    ("CNT/MDA", "2026-10-02", "Flávio Bolsonaro (PL)", 47.3, 43.1),
    ("CNT/MDA", "2026-10-02", "Augusto Cury (Avante)", 46.8, 37.9),
    ("CNT/MDA", "2026-10-02", "Ronaldo Caiado (PSD)", 47.0, 37.3),
    ("Datafolha", "2026-10-01", "Flávio Bolsonaro (PL)", 48, 45),
    ("Datafolha", "2026-10-01", "Augusto Cury (Avante)", 47, 42),
    ("Datafolha", "2026-10-01", "Ronaldo Caiado (PSD)", 48, 42),
    ("Datafolha", "2026-10-01", "Romeu Zema (Novo)", 49, 40),
    ("Datafolha", "2026-10-01", "Renan Santos (Missão)", 48, 39),
    ("Quaest", "2026-09-27", "Flávio Bolsonaro (PL)", 42, 42),
    ("Quaest", "2026-09-27", "Augusto Cury (Avante)", 42, 33),
    ("AtlasIntel", "2026-09-28", "Flávio Bolsonaro (PL)", 47.6, 47.7),
]


# Série do 1º turno (votos totais) para o gráfico de evolução: (instituto, data_fim, {candidato: %})
_C = ["Lula (PT)", "Flávio Bolsonaro (PL)", "Ronaldo Caiado (PSD)", "Romeu Zema (Novo)", "Renan Santos (Missão)",
      "Augusto Cury (Avante)"]
SERIE_1T = [(i, d, dict(zip(_C, v))) for i, d, v in [
    ("Datafolha", "2026-07-23", (40, 32, 4, 3, 3, 2)), ("Datafolha", "2026-08-20", (39, 33, 5, 3, 4, 2)),
    ("Datafolha", "2026-09-02", (38, 33, 4, 2, 3, 8)), ("Datafolha", "2026-09-10", (39, 35, 4, 2, 3, 6)),
    ("Datafolha", "2026-09-16", (39, 36, 4, 2, 3, 6)), ("Datafolha", "2026-09-24", (40, 36, 4, 1, 3, 5)),
    ("Datafolha", "2026-10-01", (42, 38, 3, 1, 3, 4)),
    ("Quaest", "2026-07-13", (40, 28, 4, 2, 3, 1)), ("Quaest", "2026-08-03", (39, 30, 4, 2, 4, 1)),
    ("Quaest", "2026-08-13", (38, 31, 4, 2, 4, 2)), ("Quaest", "2026-09-01", (37, 29, 1, 1, 3, 10)),
    ("Quaest", "2026-09-06", (36, 29, 3, 2, 3, 8)), ("Quaest", "2026-09-13", (36, 31, 4, 1, 4, 7)),
    ("Quaest", "2026-09-20", (37, 33, 4, 1, 3, 6)), ("Quaest", "2026-09-27", (39, 34, 4, 1, 3, 4)),
]]

# Aprovação do governo Lula: (instituto, data_fim, {resposta: %})
APROVACAO = [("Datafolha", "2026-10-01", {"Aprova": 48, "Desaprova": 49, "Não sabe": 3}),
             ("Quaest", "2026-09-27", {"Aprova": 46, "Desaprova": 49, "Não sabe": 5})]

# Rejeição ("não votaria de jeito nenhum"): (instituto, data_fim, {candidato: %})
REJEICAO = [
    ("Datafolha", "2026-10-01", {"Lula (PT)": 45, "Flávio Bolsonaro (PL)": 45}),
    ("Real Time Big Data", "2026-09-23", {"Ronaldo Caiado (PSD)": 38, "Romeu Zema (Novo)": 35,
                                          "Augusto Cury (Avante)": 31, "Wilson Grassi (Democrata)": 27}),
]


def _ficha(instituto: str, data_fim) -> dict | None:
    return FICHAS.get((instituto, pd.to_datetime(data_fim).strftime("%Y-%m-%d")))


RODAPE = ("Pesquisas: Datafolha, Quaest, AtlasIntel, CNT/MDA, PoderData, Nexus/BTG, Real Time Big Data, Vox Brasil, "
          "Indexa/Broadcast, Meio/Ideia e Veritá; o registro no TSE está em cada ficha. Outras fontes: Correio Braziliense/Opinião, "
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


WEB_2T: list[dict] = []   # pesquisas de 2º turno lidas da Wikipédia


def extrair_pesquisas(offline: bool) -> pd.DataFrame:
    seed = pd.DataFrame(SEED_NACIONAL, columns=["instituto", "data_fim", "lula", "flavio", "margem", "base"])
    seed["fonte"] = "snapshot"
    if offline:
        registrar("Pesquisas nacionais (Wikipédia)", False, "offline, usando snapshot")
        return seed
    try:
        html = _get(WIKI_URL, 30).text
        primeira = None
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
            col_n = next((c for c, l in cols.items() if "sample" in l), None)
            col_u = next((c for c, l in cols.items() if "undec" in l or "blank" in l), None)
            if not all([col_l, col_f, col_i, col_d]):
                continue
            outros = any(k in l for l in cols.values() for k in ("caiado", "zema", "cury", "santos", "others"))
            if not outros:
                # tabela de 2º turno Lula x Flávio: guarda só as pesquisas feitas depois do 1º turno
                for _, lin in tabela.iterrows():
                    d = _data(lin[col_d])
                    if pd.notna(d) and d.date() > DATA_1T and _pct(lin[col_l]) and _pct(lin[col_f]):
                        WEB_2T.append({"instituto": str(lin[col_i]).split("[")[0].split("/")[0].strip(), "data_fim": d,
                                       "lula": _pct(lin[col_l]), "flavio": _pct(lin[col_f]),
                                       "margem": (_pct(lin[col_m]) if col_m else None) or MARGEM_PADRAO,
                                       "entrevistas": _pct(str(lin[col_n]).replace(",", "")) if col_n else None,
                                       "fonte": "wikipedia"})
                continue
            if primeira is None:
                web = tabela[[col_i, col_d, col_l, col_f]].copy()
                web.columns = ["instituto", "data_fim", "lula", "flavio"]
                web["margem"] = tabela[col_m].map(_pct) if col_m else MARGEM_PADRAO
                web["margem"] = web["margem"].fillna(MARGEM_PADRAO)
                # pesquisas com menos de 5% de indecisos/brancos usam método diferente: ficam fora da média
                indec = tabela[col_u].map(_pct) if col_u else None
                web["base"] = "total" if indec is None else indec.map(lambda v: "baixo_indeciso" if v is not None and v < 5 else "total")
                web["fonte"] = "wikipedia"
                primeira = web
        if primeira is not None:
            registrar("Pesquisas nacionais (Wikipédia)", True, f"{len(primeira)} linhas do 1º turno, {len(WEB_2T)} do 2º")
            return pd.concat([primeira, seed], ignore_index=True)
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


def _descobrir_codigo(turno: int, tipo: str = "federal") -> str | None:
    """Procura no config do TSE a eleição geral de 2026 ("federal" = presidente; "estadual" = governador, senado).
    A estrutura pode mudar; se falhar, use --codigo-eleicao / --codigo-estadual."""
    cfg = _get(f"{TSE_RESULTADOS}/comum/config/ele-c.json", 30).json()
    achados = []

    def andar(no):
        if isinstance(no, dict):
            texto = " ".join(str(v) for v in no.values() if isinstance(v, str)).lower()
            if "cd" in no and "2026" in texto and tipo in texto:
                achados.append(no)
            for v in no.values():
                andar(v)
        elif isinstance(no, list):
            for v in no:
                andar(v)

    andar(cfg)
    achados = [a for a in achados if str(a.get("t", turno)) == str(turno)] or achados
    return str(achados[0]["cd"]) if achados else None


def _nome(nm) -> str:
    """'LEILA DO VÔLEI' -> 'Leila do Vôlei'."""
    return " ".join(w if w in ("da", "de", "do", "das", "dos", "e") else w.capitalize() for w in str(nm).lower().split())


def _partido(cc) -> str:
    """Partido ou federação do candidato, como o TSE agrupa para dividir as cadeiras.
    'PL' -> 'PL'; 'FEDERAÇÃO BRASIL DA ESPERANÇA - FE BRASIL(PT/PC do B/PV)' -> 'PT/PCdoB/PV'."""
    cc = str(cc or "").strip()
    if "(" in cc and ")" in cc:                       # federação: mostra os partidos que a compõem
        membros = cc[cc.index("(") + 1:cc.rindex(")")].replace("PC do B", "PCdoB").strip()
        if membros:
            return membros
    if " - " in cc:
        cc = cc.split(" - ", 1)[1]
    return cc.split("(")[0].strip() or "?"


def _n_tse(v) -> float:
    """Números do TSE vêm como texto: '47,03' -> 47.03; '56104503' -> 56104503; vazio -> 0."""
    if v is None or v == "":
        return 0.0
    try:
        return float(str(v).replace(".", "").replace(",", "."))
    except ValueError:
        return 0.0


def _situacao(e, st) -> str | None:
    t = str(st or "").lower()
    if "turno" in t:
        return "2turno"
    if "suplente" in t:
        return "suplente"
    if "não eleito" in t or "nao eleito" in t:
        return None
    if str(e or "").lower() == "s" or "eleito" in t:
        return "eleito"
    return None


# Códigos das eleições de 2026 no TSE (confirmados no ele-c.json oficial): 1º turno / 2º turno
COD_FEDERAL = {1: "6257", 2: "6258"}    # presidente
COD_ESTADUAL = {1: "6259", 2: "6260"}   # governador, senado, deputados


def _ler_tse(codigo: str, uf: str, cargo: int) -> dict:
    """Lê o arquivo de resultado unificado (EA20) do TSE de 2026:
    /oficial/ele2026/{eleição}/dados/{uf}/{uf}-c{cargo}-e{eleição}-u.json
    Candidatos ficam em carg[0].agr[] (agremiação/federação) -> par[] (partido) -> cand[]."""
    cod = int(codigo)
    url = f"{TSE_RESULTADOS}/ele2026/{cod}/dados/{uf}/{uf}-c{cargo:04d}-e{cod:06d}-u.json"
    j = _get(url, 30).json()
    carg = (j.get("carg") or [{}])[0]
    cands, agrs = [], []
    for agr in carg.get("agr", []):
        siglas, votos_agr = [], 0.0
        for par in agr.get("par", []):
            siglas.append(par.get("sg", ""))
            votos_agr += _n_tse(par.get("tvtn")) + _n_tse(par.get("tvtl"))      # nominais + legenda
            for c in par.get("cand", []):
                valido = not c.get("dvt") or str(c.get("dvt")).startswith("Válido")
                sit = _situacao(c.get("e"), c.get("st"))
                cands.append({"nm": _nome(c.get("nmu") or c.get("nm")), "partido": par.get("sg", ""),
                              "votos": int(_n_tse(c.get("vap"))), "pct": _n_tse(c.get("pvap")) if valido else 0.0,
                              "eleito": sit == "eleito", "segundo_turno": sit == "2turno",
                              "agr": "/".join(x for x in siglas if x) or agr.get("nm", "")})
        sig = "/".join(x for x in siglas if x)
        agrs.append({"p": sig or agr.get("nm", ""), "nome": agr.get("nm", ""), "votos": int(votos_agr),
                     "vagas": int(_n_tse(agr.get("vag"))) if agr.get("vag") not in (None, "") else None})
    cands.sort(key=lambda c: -c["votos"])
    sres = j.get("s") or {}
    pst = _n_tse(sres.get("pst"))
    return {"pst": pst, "hora": j.get("ht") or j.get("hg") or "", "cands": cands, "agrs": agrs,
            "final": j.get("tf") == "s", "definido": j.get("tf") == "s" or (pst >= 100 and j.get("md") == "s")}


def _lula_flavio(cands: list[dict]) -> dict | None:
    lula = next((c for c in cands if "LULA" in c["nm"].upper()), None)
    flav = next((c for c in cands if "BOLSONARO" in c["nm"].upper() and c.get("partido", "PL") == "PL"), None)
    if not (lula and flav):
        return None
    return {"lula_pct": lula["pct"], "flavio_pct": flav["pct"], "lula_votos": lula["votos"], "flavio_votos": flav["votos"]}


def _resumo_proporcional(r: dict) -> dict:
    """Deputados: bancada por agremiação (vagas distribuídas pelo TSE), votos e mais votados."""
    tot = sum(a["votos"] for a in r["agrs"]) or 1
    eleitos_cand = {}
    for c in r["cands"]:
        if c["eleito"]:
            eleitos_cand[c["agr"]] = eleitos_cand.get(c["agr"], 0) + 1
    partidos = []
    for a in r["agrs"]:
        el = a["vagas"] if a["vagas"] is not None else eleitos_cand.get(a["p"], 0)
        partidos.append({"p": a["p"], "votos": a["votos"], "pct": round(a["votos"] / tot * 100, 2), "eleitos": el})
    partidos.sort(key=lambda x: (-x["eleitos"], -x["votos"]))
    top = [{k: c[k] for k in ("nm", "partido", "votos", "pct", "eleito")} for c in r["cands"][:15]]
    return {"pst": r["pst"], "partidos": partidos[:20], "eleitos_total": sum(x["eleitos"] for x in partidos), "top": top}


def turno_atual() -> int:
    """2º turno a partir de 25/10/2026 (Brasília); antes disso, 1º turno."""
    return 2 if datetime.now(ZoneInfo("America/Sao_Paulo")).date() >= date(2026, 10, 25) else 1


def extrair_resultado(offline: bool, codigo: str | None, turno: int | None = None, codigo_est: str | None = None,
                      deputados: bool = True) -> dict | None:
    turno = turno or turno_atual()
    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    if offline or hoje < date(2026, 10, 4):
        registrar("Apuração oficial (TSE)", False, "ainda não começou" if not offline else "offline")
        return None
    try:
        codigo = str(codigo or COD_FEDERAL[turno])
        codigo_est = str(codigo_est or COD_ESTADUAL[turno])
        br = _ler_tse(codigo, "br", 1)
        res = _lula_flavio(br["cands"])
        if not res:
            raise ValueError("candidatos não encontrados no arquivo nacional")
        res.update({"turno": turno, "pst": br["pst"], "hora": br["hora"], "final": br["final"], "definido": br["definido"],
                    "cands": br["cands"][:12], "por_uf": {}, "estados": {},
                    "tse": {"base": TSE_RESULTADOS, "federal": str(int(codigo)), "estadual": str(int(codigo_est))}})
        for uf in UFS:
            try:
                r = _ler_tse(codigo, uf.lower(), 1)
                lf = _lula_flavio(r["cands"])
                if lf:
                    res["por_uf"][uf] = {"pst": r["pst"], **lf}
            except Exception:
                pass
        # Governador (3), Senado (5) e deputados (6; 7 ou 8 no DF) ficam na eleição estadual.
        # No 2º turno só há governador (nas UFs que tiveram 2º turno).
        cargos_maj = (("governador", 3),) if turno == 2 else (("governador", 3), ("senador", 5))
        cargos_prop = () if (turno == 2 or not deputados) else (("dep_federal", 6), ("dep_estadual", None))
        for uf in UFS:
            for nome, cargo in cargos_maj:
                try:
                    r = _ler_tse(codigo_est, uf.lower(), cargo)
                    if r["cands"]:
                        res["estados"].setdefault(uf, {})[nome] = {"pst": r["pst"], "cands": r["cands"][:8]}
                except Exception:
                    pass
            for nome, cargo in cargos_prop:
                try:
                    r = _ler_tse(codigo_est, uf.lower(), cargo or (8 if uf == "DF" else 7))
                    res["estados"].setdefault(uf, {})[nome] = _resumo_proporcional(r)
                except Exception:
                    pass
        status = (f"{turno}º turno, eleição {codigo}, " + f"{res['pst']:.2f}".replace(".", ",") + f"% das seções, "
                  f"{len(res['por_uf'])} UFs, cargos estaduais em {len(res['estados'])} UFs")
        registrar("Apuração oficial (TSE)", True, status)
        return res
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
# 2º TURNO (fase que começa no dia seguinte ao 1º turno)
# ===========================================================================
DATA_1T = date(2026, 10, 4)


def fase_atual() -> str:
    return "2t" if datetime.now(ZoneInfo("America/Sao_Paulo")).date() > DATA_1T else "1t"


# Resultado oficial do 1º turno para presidente. O pipeline tenta ler do TSE e guarda em cache/resultado_1t.json;
# estes números (TSE e Metrópoles, 04-05/10) são o plano B. uf: (votos Lula, % Lula, votos Flávio, % Flávio)
RESULTADO_1T_UF = {
    "AC": (134770, 28.73, 302807, 64.56), "AL": (995459, 54.73, 735718, 40.45), "AM": (1042340, 48.13, 976222, 45.08),
    "AP": (212503, 45.71, 212278, 45.67), "BA": (5641207, 66.12, 2438241, 28.58), "CE": (3552535, 63.28, 1756239, 31.28),
    "DF": (675627, 38.11, 909616, 51.31), "ES": (850028, 37.76, 1233194, 54.78), "GO": (1188880, 31.06, 2051888, 53.60),
    "MA": (2561100, 63.95, 1238495, 30.93), "MG": (5187467, 43.32, 5776818, 48.24), "MT": (581681, 29.18, 1298581, 65.15),
    "MS": (516855, 34.68, 873351, 58.60), "PA": (2425810, 49.90, 2163557, 44.51), "PB": (1540937, 61.31, 831205, 33.07),
    "PE": (3553971, 63.45, 1738043, 31.03), "PI": (1520985, 70.97, 516574, 24.10), "PR": (2054597, 31.20, 3945345, 59.91),
    "RJ": (3693021, 39.41, 4966844, 53.01), "RN": (1237963, 59.75, 720493, 34.77), "RS": (2294759, 35.73, 3573783, 55.64),
    "RO": (250667, 25.89, 652988, 67.45), "RR": (74420, 22.86, 231322, 71.06), "SC": (1127168, 25.04, 3000020, 66.65),
    "SE": (856019, 62.75, 417839, 30.63), "SP": (9505413, 38.20, 12922023, 51.93), "TO": (404009, 43.42, 469321, 50.44),
}
RESULTADO_1T_BR = {
    "lula_votos": 53879538, "lula_pct": 45.16, "flavio_votos": 56104503, "flavio_pct": 47.03,
    "validos": 119300788, "brancos": 2300798, "nulos": 3674249, "abstencao": 33469244, "abstencao_pct": 21.08,
    "outros": [("Augusto Cury", "Avante", 3448569, 2.89), ("Renan Santos", "Missão", 2675887, 2.24),
               ("Ronaldo Caiado", "PSD", 2605148, 2.18), ("Romeu Zema", "Novo", 326488, 0.27),
               ("Samara Martins", "UP", 122911, 0.10), ("Hertz Dias", "PSTU", 43103, 0.04),
               ("Clariana Barão", "DC", 40043, 0.03), ("Edmilson Costa", "PCB", 22693, 0.02),
               ("Wilson Grassi", "Democrata", 16881, 0.01), ("Rui Costa Pimenta", "PCO", 15024, 0.01)],
}

# Governadores no 2º turno: uf, (nome, partido, campo, % no 1º turno) x 2.  campo: L = aliado de Lula, F = de Flávio, N = nenhum
GOV_2T = [
    ("AC", ("Mailza Assis", "PP", "F", 49.76), ("Alan Rick", "Republicanos", "F", 32.27)),
    ("AM", ("Omar Aziz", "PSD", "L", 40.38), ("Maria do Carmo", "PL", "F", 24.72)),
    ("DF", ("Celina Leão", "PP", "F", 49.93), ("Leandro Grass", "PT", "L", 34.48)),
    ("ES", ("Lorenzo Pazolini", "Republicanos", "N", 49.65), ("Ricardo Ferraço", "MDB", "N", 34.06)),
    ("RJ", ("Douglas Ruas", "PL", "F", 49.27), ("Eduardo Paes", "PSD", "L", 42.76)),
    ("RN", ("Allyson Bezerra", "União", "N", 36.83), ("Cadu de Lula", "PT", "L", 36.21)),
    ("TO", ("Professora Dorinha", "União", "N", 45.52), ("Vicentinho Júnior", "PSDB", "N", 43.94)),
]

# Pesquisas de 2º turno (Lula x Flávio), feitas depois de 4/10. A Wikipédia é lida sozinha; aqui vão as do snapshot.
# (instituto, data_fim, lula, flavio, margem, entrevistas)
PESQUISAS_2T = []

# Transferência de votos (quando as pesquisas perguntarem): eliminado -> {"Lula": %, "Flávio": %, "fonte": "..."}
TRANSFERENCIA = {}

MEIA_VIDA_DIAS = 4      # peso da pesquisa cai pela metade a cada 4 dias
JANELA_2T_DIAS = 14


def resultado_1t(offline: bool) -> dict:
    """Resultado oficial do 1º turno (presidente), do TSE quando possível; senão, o plano B acima."""
    cache = CACHE / "resultado_1t.json"
    if cache.exists():
        try:
            return json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            pass
    br = dict(RESULTADO_1T_BR, outros=[list(x) for x in RESULTADO_1T_BR["outros"]], fonte="TSE (plano B do script)")
    por_uf = {u: {"lula_votos": v[0], "lula_pct": v[1], "flavio_votos": v[2], "flavio_pct": v[3]}
              for u, v in RESULTADO_1T_UF.items()}
    if not offline:
        try:
            r = _ler_tse(COD_FEDERAL[1], "br", 1)
            lf = _lula_flavio(r["cands"])
            if lf and r["pst"] >= 99.9:
                br.update(lf)
                br["outros"] = [[c["nm"], c["partido"], c["votos"], c["pct"]] for c in r["cands"]
                                if "LULA" not in c["nm"].upper() and "BOLSONARO" not in c["nm"].upper()]
                br["fonte"] = "TSE"
                novos = {}
                for u in UFS:
                    x = _lula_flavio(_ler_tse(COD_FEDERAL[1], u.lower(), 1)["cands"])
                    if x:
                        novos[u] = x
                if len(novos) == len(UFS):
                    por_uf = novos
                    CACHE.mkdir(exist_ok=True)
                    cache.write_text(json.dumps({"br": br, "por_uf": por_uf}, ensure_ascii=False), encoding="utf-8")
        except Exception as erro:
            print(f"[1º turno] usando plano B ({str(erro)[:60]})")
    return {"br": br, "por_uf": por_uf}


def media_ponderada_2t(linhas: list[dict]) -> dict | None:
    """Uma pesquisa por instituto (a mais recente), peso = recência (meia-vida de 4 dias) x raiz(entrevistas/1000)."""
    if not linhas:
        return None
    df = pd.DataFrame(linhas).sort_values("data_fim", ascending=False).drop_duplicates("instituto")
    ref = df["data_fim"].max()
    df = df[df["data_fim"] >= ref - pd.Timedelta(days=JANELA_2T_DIAS)]
    dias = (ref - df["data_fim"]).dt.days
    peso = (0.5 ** (dias / MEIA_VIDA_DIAS)) * ((df["entrevistas"].fillna(2000) / 1000) ** 0.5)
    return {"lula": round(float((df["lula"] * peso).sum() / peso.sum()), 1),
            "flavio": round(float((df["flavio"] * peso).sum() / peso.sum()), 1), "n": int(len(df)),
            "pesos": {i: round(float(w / peso.sum() * 100)) for i, w in zip(df["instituto"], peso)}}


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
    df["instituto"] = df["instituto"].replace({"Real Time": "Real Time Big Data", "Genial": "Quaest", "Meio": "Ideia", "CNT": "CNT/MDA", "MDA": "CNT/MDA"})
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
    df["tem_ficha"] = [(_ficha(i, d) is not None) for i, d in zip(df["instituto"], df["data_fim"])]
    base_media = df[df["tem_ficha"]] if PUBLICO else df
    recentes = base_media[(base_media["data_fim"] >= corte) & (base_media["base"] == "total")].drop_duplicates("instituto")
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


def _aplicar_fase_2t(dados: dict, r1t: dict) -> None:
    """Depois do 1º turno: mapa e saldo passam a usar o resultado real das urnas; entram as pesquisas de 2º turno."""
    pu = r1t["por_uf"]
    for u in dados["ufs"]:
        x = pu.get(u["uf"])
        if not x:
            continue
        u.update({"l1": x["lula_pct"], "f1": x["flavio_pct"], "fonte1": "urnas, 1º turno",
                  "l26": "L" if x["lula_pct"] > x["flavio_pct"] else "F",
                  "saldo1": int(x["lula_votos"] - x["flavio_votos"]), "lula_votos": x["lula_votos"],
                  "flavio_votos": x["flavio_votos"], "l2": None, "f2": None, "lider2": "?", "saldo2": None, "nota": None})
    reg = {}
    for u in dados["ufs"]:
        x = pu.get(u["uf"])
        if not x:
            continue
        validos = (x["lula_votos"] + x["flavio_votos"]) / max(0.01, (x["lula_pct"] + x["flavio_pct"]) / 100)
        r = reg.setdefault(u["regiao"], [u["regiao"], 0, 0, 0, 0])
        r[1] += int(validos); r[2] += int(x["lula_votos"]); r[3] += int(x["flavio_votos"])
    dados["reg"] = list(reg.values())
    dados["r1t"] = r1t["br"]
    dados["gov2t"] = [{"uf": uf, "a": {"nm": a[0], "p": a[1], "campo": a[2], "pct": a[3]},
                       "b": {"nm": b[0], "p": b[1], "campo": b[2], "pct": b[3]}} for uf, a, b in GOV_2T]
    linhas = [{"instituto": i, "data_fim": pd.to_datetime(d), "lula": l, "flavio": f, "margem": m, "entrevistas": n,
               "fonte": "snapshot"} for i, d, l, f, m, n in PESQUISAS_2T if pd.to_datetime(d).date() > DATA_1T] + WEB_2T
    vistas = {}
    for x in sorted(linhas, key=lambda x: x["fonte"] != "snapshot"):
        vistas.setdefault((x["instituto"], pd.to_datetime(x["data_fim"]).strftime("%Y-%m-%d")), x)
    linhas = sorted(vistas.values(), key=lambda x: pd.to_datetime(x["data_fim"]), reverse=True)
    dados["media2t"] = media_ponderada_2t([dict(x, data_fim=pd.to_datetime(x["data_fim"])) for x in linhas])
    dados["pesq2t"] = [{"i": x["instituto"], "d": f"{pd.to_datetime(x['data_fim']).day}/{pd.to_datetime(x['data_fim']).month}",
                        "l": x["lula"], "f": x["flavio"], "m": x["margem"], "n": x["entrevistas"]} for x in linhas[:12]]
    dados["transf"] = TRANSFERENCIA


def publicar(pesq, uf, gov, reg, seg, media, resultado, atualizado: str, noticias=None, criterio="", r1t=None) -> Path:
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
        linhas = [{"uf": "BR", **{k: v for k, v in resultado.items() if k not in ("por_uf", "cands", "estados", "tse")}}]
        linhas += [{"uf": k, **v} for k, v in resultado["por_uf"].items()]
        pd.DataFrame(linhas).to_csv(SAIDA / "fato_apuracao_tse.csv", **op)

    dia = lambda d: f"{d.day}/{d.month}"
    dados = {
        "atualizado": atualizado, "eleicao": DATA_ELEICAO, "comparecimento": COMPARECIMENTO, "media": media,
        "pesquisas": [{"i": r.instituto, "d": dia(r.data_fim), "l": r.lula, "f": r.flavio, "base": r.base,
                       "ficha": _ficha(r.instituto, r.data_fim)}
                      for r in (pesq[pesq["tem_ficha"]] if PUBLICO else pesq).drop_duplicates("instituto").head(10).itertuples()],
        "publico": PUBLICO,
        "candidatos": [{"i": i, "d": dia(pd.to_datetime(d)), "ficha": FICHAS.get((i, d)), "votos": v,
                        "base": "baixo_indeciso" if i in ("AtlasIntel", "Palver") else "total"}
                       for (i, d), v in CANDIDATOS_1T.items() if (not PUBLICO or (i, d) in FICHAS)],
        "serie": [{"i": i, "d": dia(pd.to_datetime(d)), "iso": d, "votos": v, "ficha": FICHAS.get((i, d))}
                  for i, d, v in SERIE_1T if (not PUBLICO or (i, d) in FICHAS)],
        "aprovacao": [{"i": i, "d": dia(pd.to_datetime(d)), "v": v, "ficha": FICHAS.get((i, d))}
                      for i, d, v in APROVACAO if (not PUBLICO or (i, d) in FICHAS)],
        "rejeicao": [{"i": i, "d": dia(pd.to_datetime(d)), "v": v, "ficha": FICHAS.get((i, d))}
                     for i, d, v in REJEICAO if (not PUBLICO or (i, d) in FICHAS)],
        "cenarios2t": [{"i": i, "d": dia(pd.to_datetime(d)), "adv": a, "l": l, "o": o}
                       for i, d, a, l, o in CENARIOS_2T if (not PUBLICO or (i, d) in FICHAS)],
        "seg2t": [{"i": r.instituto, "d": dia(pd.to_datetime(r.data_fim)), "l": r.lula, "f": r.flavio,
                   "base": "baixo_indeciso" if r.instituto == "AtlasIntel" else "total",
                   "ficha": _ficha(r.instituto, r.data_fim)}
                  for r in seg.sort_values("data_fim", ascending=False).itertuples()
                  if (not PUBLICO or _ficha(r.instituto, r.data_fim))],
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
    dados["fase"] = fase_atual()
    if dados["fase"] == "2t" and r1t:
        _aplicar_fase_2t(dados, r1t)
    html = TEMPLATE.read_text(encoding="utf-8").replace("/*__DADOS__*/null", json.dumps(dados, ensure_ascii=False, default=str))
    destino = SAIDA / NOME_SAIDA
    destino.write_text(html, encoding="utf-8")

    com = uf.dropna(subset=["saldo_1t"])
    print(f"\n=== ELEIÇÕES 2026 – {atualizado} ===")
    print(f"Média: Lula {media['lula']}% x Flávio {media['flavio']}% ({media['n']} pesquisas)")
    print(f"Saldo líquido estimado: {com['saldo_1t'].sum()/1e6:+.2f} mi (positivo = Lula)")
    if resultado:
        print(f"APURAÇÃO TSE: {resultado['pst']:.2f}% das seções | Lula {resultado['lula_pct']}% x Flávio {resultado['flavio_pct']}%")
    print(f"Painel: {destino}")
    return destino


def apuracao_ao_vivo(args) -> None:
    """Grava apuracao.json na raiz do repositório; a página lê esse arquivo a cada ~1 minuto.
    Deputados são listas grandes: só são relidos com --com-deputados; senão, mantém os da leitura anterior."""
    destino = AQUI / "apuracao.json"
    anterior = {}
    if destino.exists():
        try:
            anterior = json.loads(destino.read_text(encoding="utf-8")).get("resultado") or {}
        except Exception:
            anterior = {}
    res = extrair_resultado(False, args.codigo_eleicao or None, args.turno, args.codigo_estadual or None,
                            deputados=args.com_deputados)
    if not res:
        print("[ao vivo] TSE sem dados ainda")
        return
    if not args.com_deputados:
        for uf, ant in (anterior.get("estados") or {}).items():
            for casa in ("dep_federal", "dep_estadual"):
                if casa in ant:
                    res["estados"].setdefault(uf, {})[casa] = ant[casa]
    agora = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y às %Hh%M")
    destino.write_text(json.dumps({"atualizado": agora, "resultado": res}, ensure_ascii=False), encoding="utf-8")
    print(f"[ao vivo] {agora}: {res['pst']:.2f}% das seções | Lula {res['lula_pct']}% x Flávio {res['flavio_pct']}%")


def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline diário das eleições 2026")
    ap.add_argument("--offline", action="store_true", help="não acessa a internet; usa só o snapshot")
    ap.add_argument("--completo", action="store_true",
                    help="gera a visão completa (pesquisas estaduais sem ficha incluídas) em saida/painel_completo.html; "
                         "uso pessoal, não publique")
    ap.add_argument("--codigo-eleicao", help="código da eleição no TSE (ex.: 619), se a descoberta automática falhar")
    ap.add_argument("--codigo-estadual", help="código da eleição estadual no TSE (governador/senado), se a descoberta falhar")
    ap.add_argument("--turno", type=int, default=None, choices=[1, 2], help="padrão: automático pela data")
    ap.add_argument("--so-apuracao", action="store_true",
                    help="modo ao vivo: lê só a apuração do TSE e grava apuracao.json (usado no dia da eleição)")
    ap.add_argument("--com-deputados", action="store_true", help="no modo ao vivo, relê também os deputados (mais lento)")
    args = ap.parse_args()
    global PUBLICO, NOME_SAIDA
    if args.completo:
        PUBLICO, NOME_SAIDA = False, "painel_completo.html"

    if args.so_apuracao:
        apuracao_ao_vivo(args)
        return

    eleitorado = extrair_eleitorado(args.offline)
    bruto = extrair_pesquisas(args.offline)
    resultado = extrair_resultado(args.offline, args.codigo_eleicao, args.turno, args.codigo_estadual)
    noticias, criterio = classificar_noticias(extrair_noticias(args.offline))
    pesq, uf, gov, reg, seg, media = agregar(tratar(bruto), eleitorado)
    r1t = resultado_1t(args.offline) if fase_atual() == "2t" else None
    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    publicar(pesq, uf, gov, reg, seg, media, resultado, agora.strftime("%d/%m/%Y às %Hh%M"), noticias, criterio, r1t)


if __name__ == "__main__":
    main()

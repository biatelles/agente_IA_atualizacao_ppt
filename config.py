import os

# ==========================================================
# PATHS
# ==========================================================

TEMPLATE_PATH = r"C:\Python DSA\Agentes de IA\(UNIFAA) Expansão Barra Mansa_VF (1).pptx"
OUTPUT_DIR    = r"C:\Python DSA\Agentes de IA\output"

# Censo Escolar
CENSO_PATH = r"C:\Python DSA\Agentes de IA\Censo Escolar_10-25_Resumido.xlsx"

# base_completa: Código INEP → Microárea / Município / Estado
BASE_COMPLETA_PATH          = r"C:\Python DSA\Agentes de IA\base_completa.xlsx"
BASE_COMPLETA_COL_INEP      = "Código INEP"
BASE_COMPLETA_COL_MICROAREA = "Microárea"
BASE_COMPLETA_COL_MUNICIPIO = "Município"
BASE_COMPLETA_COL_ESTADO    = "Estado"

# Microáreas: dados demográficos por microárea (CSV separado por ;)
MICROAREAS_PATH              = r"C:\Python DSA\Agentes de IA\Microáreas.xlsx"
MICROAREAS_ENCODING          = "latin-1"
MICROAREAS_SEP               = ";"
MICROAREAS_COL_MICROAREA     = "Microáreas"
MICROAREAS_COL_MUNICIPIO     = "Município"
MICROAREAS_COL_ESTADO        = "Estado"
MICROAREAS_COL_POPULACAO     = "População"
MICROAREAS_COL_RENDA_MEDIA   = "Renda Média"
MICROAREAS_COL_POP_ATE9      = "População por Faixa Etária - Até 9"
MICROAREAS_COL_POP_10_14     = "População por Faixa Etária - 10 a 14"
MICROAREAS_COL_POP_15_19     = "População por Faixa Etária - 15 a 19"
MICROAREAS_CLASSES           = ["A++", "A+", "B1", "B2", "C1", "C2", "D", "E"]
MICROAREAS_COL_CLASSE_PREFIX = "População por Faixa de Renda vs Faixa Etária - "  # usado para calcular % renda

# Colunas de POPULAÇÃO por classe (para % de pessoas por classe social)
MICROAREAS_COL_POP_CLASSE_PREFIX = "População por Faixa de Renda vs Faixa Etária - "
MICROAREAS_POP_CLASSES = ["A++", "A+", "B1", "B2", "C1", "C2", "D", "E"]

# ==========================================================
# CONFIG OPENAI
# ==========================================================

OPENAI_MODEL      = "gpt-4o"
OPENAI_MAX_TOKENS = 2000

# ==========================================================
# COLUNAS CENSO
# ==========================================================

COL_MUNICIPIO   = "NO_MUNICIPIO"
COL_ANO         = "NU_ANO_CENSO"
COL_ESCOLA      = "NO_ENTIDADE"
COL_DEPENDENCIA = "TP_DEPENDENCIA"
VAL_PRIVADA     = "Privada"

COLUNAS_MATRICULAS = {
    "QT_MAT_INF":     "Infantil",
    "QT_MAT_FUND_AI": "Fundamental I",
    "QT_MAT_FUND_AF": "Fundamental II",
    "QT_MAT_MED":     "Médio",
}

# ==========================================================
# ANOS
# ==========================================================

CENSO_ANO_MIN              = 2010
CENSO_ANO_MAX              = 2025
CENSO_ANOS_EVOLUCAO_PADRAO = 10

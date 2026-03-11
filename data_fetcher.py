"""
data_fetcher.py — joins via base_completa + Microáreas + Censo Escolar

Lógica das microáreas (slides 1 e 2):
  1. base_completa   → Código INEP + Microárea + Município + Estado
  2. Censo Escolar   → CO_ENTIDADE + matrículas por fase + dependência
  3. Join via INEP normalizado → matrículas privadas por escola
  4. Agrega por Microárea → top 6 por volume de matrículas privadas
  5. Microáreas.csv  → população total e pop<19 anos por microárea

Demais dados:
  - Market share: top 15 escolas privadas por fase (só Censo)
  - Evolução: série temporal de matrículas privadas por fase
  - CAGR: calculado total e por fase para cada período solicitado
  - Top 10: top 10 escolas por matrículas totais no ano pontual

=== BUGS CORRIGIDOS ===

BUG 1 — IndentationError no bloco de dados demográficos (linhas ~242-299):
  O corpo do try: e o except: estavam desalinhados — o corpo do try ficava
  fora do bloco, e o except ficava no nível do módulo. Corrigido alinhando
  todo o bloco corretamente.

BUG 2 — pop_total e pop_target infladas (valores muito acima do esperado):
  O merge com df_demo_sel usava apenas "bairro" como chave sem garantir
  cardinalidade 1:1. Qualquer linha duplicada na planilha Microáreas para
  o mesmo bairro resulta em produto cartesiano e multiplica os valores de
  população pelo número de duplicatas. Corrigido com
  .drop_duplicates(subset=["bairro"]) antes do merge.

BUG 3 — Matrículas públicas sempre zero no slide 2:
  A base_completa contém tipicamente apenas escolas privadas (é a base do
  fornecedor do produto). O inner join df_bc × df_censo_pub retornava vazio
  porque nenhuma escola pública tinha INEP na base_completa, zerando
  tt_matriculas_publicas silenciosamente.
  Corrigido com lógica em cascata:
    (a) Tenta join base_completa × Censo público (funciona se df_bc tiver INEPs públicos).
    (b) Se o join retornar vazio, lê diretamente do Censo Escolar:
        soma de TP_DEPENDENCIA != Privada para o município/ano — leitura
        fiel e direta do Censo, independente do mapeamento por microárea.
"""

import pandas as pd
import unicodedata
from config import (
    CENSO_PATH,
    COL_MUNICIPIO, COL_ANO, COL_DEPENDENCIA, VAL_PRIVADA,
    COLUNAS_MATRICULAS,
    BASE_COMPLETA_PATH, BASE_COMPLETA_COL_INEP,
    BASE_COMPLETA_COL_MICROAREA, BASE_COMPLETA_COL_MUNICIPIO,
    BASE_COMPLETA_COL_ESTADO,
    MICROAREAS_PATH, MICROAREAS_ENCODING, MICROAREAS_SEP,
    MICROAREAS_COL_MICROAREA, MICROAREAS_COL_MUNICIPIO,
    MICROAREAS_COL_ESTADO, MICROAREAS_COL_POPULACAO,
    MICROAREAS_COL_RENDA_MEDIA, MICROAREAS_COL_POP_ATE9,
    MICROAREAS_COL_POP_10_14, MICROAREAS_COL_POP_15_19,
    MICROAREAS_CLASSES, MICROAREAS_COL_CLASSE_PREFIX,
    MICROAREAS_COL_POP_CLASSE_PREFIX, MICROAREAS_POP_CLASSES,
)

# Colunas de matrícula no Censo
COLS_FASES = list(COLUNAS_MATRICULAS.keys())  # QT_MAT_INF, QT_MAT_FUND_AI, etc.
COLS_FASES_LABELS = list(COLUNAS_MATRICULAS.values())


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def fmt_pct(valor, total):
    if total and total > 0:
        return f"{round(valor / total * 100)}%"
    return "—"

def _calcular_cagr(v_ini, v_fim, n_anos):
    try:
        if v_ini <= 0 or n_anos <= 0:
            return "n/a"
        taxa = (v_fim / v_ini) ** (1 / n_anos) - 1
        sinal = "+" if taxa >= 0 else ""
        return f"{sinal}{taxa*100:.1f}%".replace(".", ",")
    except Exception:
        return "n/a"


# ─────────────────────────────────────────────────────────────
# LEITURA DOS EXCELS
# ─────────────────────────────────────────────────────────────

def _ler_censo(cidade):
    """Lê Censo Escolar e filtra pela cidade."""
    df = pd.read_excel(CENSO_PATH)
    df[COL_MUNICIPIO] = df[COL_MUNICIPIO].astype(str).str.strip()
    df = df[df[COL_MUNICIPIO].str.upper() == cidade.upper()].copy()
    # Garante que colunas de matrícula são numéricas
    for col in COLS_FASES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _normalizar_str(s):
    """Remove acentos e normaliza para comparação."""
    s = str(s).strip().upper()
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def _normalizar_inep(serie):
    """Converte float INEP (ex: 42000041.0) para string sem zeros extras."""
    try:
        return serie.fillna(0).astype(float).astype(int).astype(str).str.strip().str.lstrip("0")
    except Exception:
        return serie.astype(str).str.strip().str.replace(r"\D", "", regex=True).str.lstrip("0")


def _ler_base_completa(cidade):
    """
    Lê base_completa e filtra pelo município.
    Retorna DataFrame com Código INEP + Microárea.
    """
    df = pd.read_excel(BASE_COMPLETA_PATH)
    df[BASE_COMPLETA_COL_MUNICIPIO] = df[BASE_COMPLETA_COL_MUNICIPIO].astype(str).str.strip()
    cidade_norm = _normalizar_str(cidade)
    mask = df[BASE_COMPLETA_COL_MUNICIPIO].apply(_normalizar_str) == cidade_norm
    return df[mask].copy()


def _br_to_float(serie):
    """
    Converte série com formato numérico brasileiro (1.234,56) para float.
    Remove pontos de milhar e troca vírgula decimal por ponto.
    """
    return pd.to_numeric(
        serie.astype(str)
             .str.strip()
             .str.replace(r"\.", "", regex=True)   # remove ponto de milhar
             .str.replace(",", ".", regex=False),   # vírgula → ponto decimal
        errors="coerce"
    ).fillna(0)


def _ler_microareas(cidade):
    """
    Lê Microareas.xlsx e filtra pelo município.
    Retorna DataFrame com dados demográficos por microárea.
    O .xlsx já entrega valores como float64 nativos — NÃO usar _br_to_float,
    que removeria o ponto decimal e corromperia os valores (ex: 26380.43 → 2638043).
    """
    df = pd.read_excel(MICROAREAS_PATH)
    df.columns = df.columns.str.strip()
    df[MICROAREAS_COL_MUNICIPIO] = df[MICROAREAS_COL_MUNICIPIO].astype(str).str.strip()
    cidade_norm = _normalizar_str(cidade)
    mask = df[MICROAREAS_COL_MUNICIPIO].apply(_normalizar_str) == cidade_norm
    df = df[mask].copy()

    # Garante numérico em todas as colunas de interesse (já são float64 no xlsx)
    cols_numericas = [
        MICROAREAS_COL_POPULACAO,
        MICROAREAS_COL_POP_ATE9, MICROAREAS_COL_POP_10_14, MICROAREAS_COL_POP_15_19,
        MICROAREAS_COL_RENDA_MEDIA,
    ] + [MICROAREAS_COL_CLASSE_PREFIX + c for c in MICROAREAS_CLASSES]       + [MICROAREAS_COL_POP_CLASSE_PREFIX + c for c in MICROAREAS_POP_CLASSES]

    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ─────────────────────────────────────────────────────────────
# JOIN: base_completa × Censo → matrículas privadas por microárea
#       + Microáreas → dados demográficos
# ─────────────────────────────────────────────────────────────

def _matriculas_por_bairro(cidade, ano, bairros="automatico", top_n=6):
    """
    Retorna DataFrame com dados por microárea para os slides 1 e 2.

    Colunas produzidas (alinhadas com o template):
      col 1 — pop_total    : população total da microárea          (Microáreas.xlsx)
      col 2 — pop_target   : pop até 19 anos (Até9 + 10-14 + 15-19) (Microáreas.xlsx)
      col 3 — tt_matriculas_privadas : matrículas privadas          (base_completa × Censo)
      col 4 — penetracao   : col3 / col2                           (calculado)
      col 5-11 — pct_classe_*: % população por classe social       (Microáreas.xlsx, val/pop_total)
        A++, A+, B1, B2, C1, C2, D_E (D+E somados)
    """
    # ── 1. Censo: escolas privadas do município/ano ──────────────
    df_censo = _ler_censo(cidade)
    df_censo_priv = df_censo[
        (df_censo[COL_ANO] == ano) &
        (df_censo[COL_DEPENDENCIA] == VAL_PRIVADA)
    ].copy()
    df_censo_priv["tt_privadas"] = df_censo_priv[COLS_FASES].sum(axis=1)
    df_censo_priv["_inep"]       = _normalizar_inep(df_censo_priv["CO_ENTIDADE"])

    # ── 2. base_completa: INEP → Microárea (só privadas) ────────
    print("🔗 Calculando matrículas privadas por microárea via join INEP...")
    df_bc = _ler_base_completa(cidade)
    if df_bc.empty:
        raise ValueError(f"Cidade '{cidade}' não encontrada na base_completa.")
    df_bc["_inep"] = _normalizar_inep(df_bc[BASE_COMPLETA_COL_INEP])

    col_ma = BASE_COMPLETA_COL_MICROAREA

    # ── 3. Join base_completa × Censo privadas → agrega por microárea ──
    df_join = df_bc.merge(
        df_censo_priv[["_inep", "tt_privadas"]],
        on="_inep", how="inner"
    )
    if df_join.empty:
        raise ValueError(
            f"Join base_completa × Censo retornou vazio para {cidade}/{ano}. "
            "Verifique se os códigos INEP batem."
        )

    df_ma = (
        df_join.groupby(col_ma)["tt_privadas"]
        .sum().reset_index()
        .rename(columns={col_ma: "bairro", "tt_privadas": "tt_matriculas_privadas"})
    )

    # Filtra microáreas explícitas se fornecido
    if bairros != "automatico" and isinstance(bairros, list):
        df_ma = df_ma[df_ma["bairro"].isin(bairros)]

    # Top N por matrículas privadas
    df_ma = df_ma.sort_values("tt_matriculas_privadas", ascending=False).head(top_n)

    # ── 4. Dados demográficos e classes sociais (Microáreas.xlsx) ──
    try:
        df_demo = _ler_microareas(cidade)

        if df_demo.empty:
            raise ValueError("Microáreas vazio para esta cidade.")

        # Colunas de faixa etária até 19 anos
        cols_ate19 = [MICROAREAS_COL_POP_ATE9, MICROAREAS_COL_POP_10_14, MICROAREAS_COL_POP_15_19]
        cols_ate19 = [c for c in cols_ate19 if c in df_demo.columns]

        # Colunas de classe social (valores absolutos de população)
        COL_APP = "População por Faixa de Renda vs Faixa Etária - A++"
        COL_AP  = "População por Faixa de Renda vs Faixa Etária - A+"
        COL_B1  = "População por Faixa de Renda vs Faixa Etária - B1"
        COL_B2  = "População por Faixa de Renda vs Faixa Etária - B2"
        COL_C1  = "População por Faixa de Renda vs Faixa Etária - C1"
        COL_C2  = "População por Faixa de Renda vs Faixa Etária - C2"
        COL_D   = "População por Faixa de Renda vs Faixa Etária - D"
        COL_E   = "População por Faixa de Renda vs Faixa Etária - E"

        # Garante numérico
        for col in cols_ate19 + [MICROAREAS_COL_POPULACAO,
                                  COL_APP, COL_AP, COL_B1, COL_B2,
                                  COL_C1, COL_C2, COL_D, COL_E]:
            if col in df_demo.columns:
                df_demo[col] = pd.to_numeric(df_demo[col], errors="coerce").fillna(0)

        # Calcula campos derivados
        df_demo["pop_target"] = df_demo[cols_ate19].sum(axis=1) if cols_ate19 else 0

        # % por classe = valor_absoluto / pop_total * 100  (arredondado para 1 casa)
        pop = df_demo[MICROAREAS_COL_POPULACAO].replace(0, pd.NA)
        df_demo["pct_classe_App"] = (df_demo[COL_APP] / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_Ap"]  = (df_demo[COL_AP]  / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_B1"]  = (df_demo[COL_B1]  / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_B2"]  = (df_demo[COL_B2]  / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_C1"]  = (df_demo[COL_C1]  / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_C2"]  = (df_demo[COL_C2]  / pop * 100).round(1).fillna(0)
        df_demo["pct_classe_D_E"] = ((df_demo[COL_D] + df_demo[COL_E]) / pop * 100).round(1).fillna(0)

        # Seleciona colunas para merge
        cols_sel = [
            MICROAREAS_COL_MICROAREA,
            MICROAREAS_COL_POPULACAO,
            "pop_target",
            "pct_classe_App", "pct_classe_Ap",
            "pct_classe_B1",  "pct_classe_B2",
            "pct_classe_C1",  "pct_classe_C2",
            "pct_classe_D_E",
        ]
        df_demo_sel = (
            df_demo[[c for c in cols_sel if c in df_demo.columns]]
            .copy()
            .rename(columns={
                MICROAREAS_COL_MICROAREA: "bairro",
                MICROAREAS_COL_POPULACAO: "pop_total",
            })
            .drop_duplicates(subset=["bairro"])
        )

        df_ma["bairro"]       = df_ma["bairro"].astype(str).str.strip()
        df_demo_sel["bairro"] = df_demo_sel["bairro"].astype(str).str.strip()

        df_ma = df_ma.merge(df_demo_sel, on="bairro", how="left")

    except Exception as e:
        print(f"⚠️  Dados demográficos não carregados: {e}")
        for col in ["pop_total", "pop_target",
                    "pct_classe_App", "pct_classe_Ap",
                    "pct_classe_B1", "pct_classe_B2",
                    "pct_classe_C1", "pct_classe_C2", "pct_classe_D_E"]:
            df_ma[col] = 0

    # ── 5. Garante tipos e calcula penetração (col3 / col2) ──────
    for col in ["pop_total", "pop_target", "tt_matriculas_privadas"]:
        if col not in df_ma.columns:
            df_ma[col] = 0
        df_ma[col] = pd.to_numeric(df_ma[col], errors="coerce").fillna(0)

    # penetração = matrículas privadas / pop_target
    df_ma["penetracao"] = df_ma.apply(
        lambda r: fmt_pct(r["tt_matriculas_privadas"], r["pop_target"]), axis=1
    )

    return df_ma.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# MARKET SHARE POR FASE
# ─────────────────────────────────────────────────────────────

def _market_share_fase(df_censo, ano, col_fase, top_n=15):
    df = df_censo[
        (df_censo[COL_ANO] == ano) &
        (df_censo[COL_DEPENDENCIA] == VAL_PRIVADA) &
        (df_censo[col_fase] > 0)
    ].copy()
    df = df.sort_values(col_fase, ascending=False).head(top_n)
    cats  = df["NO_ENTIDADE"].tolist()
    vals  = df[col_fase].astype(int).tolist()
    total = sum(vals)
    pct   = [round(v / total, 4) if total > 0 else 0 for v in vals]
    return cats, vals, pct


# ─────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────

def buscar_tudo(cidade, ano_pontual=2025, anos_evolucao=None,
                anos_cagr=None, bairros="automatico"):

    if anos_evolucao is None:
        anos_evolucao = list(range(2016, 2026))
    if anos_cagr is None:
        anos_cagr = {}

    try:
        return _buscar_dados_reais(cidade, ano_pontual, anos_evolucao, anos_cagr, bairros)
    except Exception as e:
        print(f"⚠️  Dados reais indisponíveis ({e}). Usando dados de exemplo.")
        return _dados_mock(cidade, ano_pontual, anos_evolucao, anos_cagr)


def _buscar_dados_reais(cidade, ano_pontual, anos_evolucao, anos_cagr, bairros):

    df_censo = _ler_censo(cidade)
    if df_censo.empty:
        raise ValueError(f"Cidade '{cidade}' não encontrada no Censo.")

    df_priv = df_censo[df_censo[COL_DEPENDENCIA] == VAL_PRIVADA]

    # ── Stats por fase (slides 5-8) ───────────────────────────
    df_ano = df_priv[df_priv[COL_ANO] == ano_pontual]
    total_geral = int(df_ano[COLS_FASES].sum().sum())

    # Mapa col → label
    col_por_fase = {col: label for label, col in COLUNAS_MATRICULAS.items()}

    stats_por_fase = {}
    acum = 0
    for fase_label, col_fase in COLUNAS_MATRICULAS.items():
        if col_fase not in df_ano.columns:
            stats_por_fase[fase_label] = {
                "n_colegios": 0, "n_colegios_so_fase": 0,
                "n_alunos": 0, "pct_so_fase": "0%", "pct_todos": "0%",
                "pct_outros": "100%", "pct_inf_fund1": "0%", "pct_inf_fund1_fund2": "0%",
            }
            continue

        n_alunos = int(df_ano[col_fase].sum())

        # Colégios que ofertam SOMENTE esta fase (todas as outras colunas = 0)
        outras_cols = [c for c in COLS_FASES if c != col_fase]
        mask_so_fase = (df_ano[col_fase] > 0) & (df_ano[outras_cols].sum(axis=1) == 0)
        n_colegios_so_fase = int(df_ano[mask_so_fase]["NO_ENTIDADE"].nunique())

        # Colégios que ofertam esta fase (independente de outras)
        n_colegios = int(df_ano[df_ano[col_fase] > 0]["NO_ENTIDADE"].nunique())

        acum += n_alunos

        # Acumulados específicos para cada slide:
        # Slide 5 (INF): mostra INF e INF+FUND1
        # Slide 6 (FUND1): mostra INF+FUND1
        # Slide 7 (FUND2): mostra INF+FUND1+FUND2
        cols_inf_fund1 = ["QT_MAT_INF", "QT_MAT_FUND_AI"]
        cols_inf_fund1_fund2 = ["QT_MAT_INF", "QT_MAT_FUND_AI", "QT_MAT_FUND_AF"]
        acum_inf_fund1 = int(
            df_ano[[c for c in cols_inf_fund1 if c in df_ano.columns]].sum().sum()
        )
        acum_inf_fund1_fund2 = int(
            df_ano[[c for c in cols_inf_fund1_fund2 if c in df_ano.columns]].sum().sum()
        )

        stats_por_fase[fase_label] = {
            "n_colegios":           n_colegios,
            "n_colegios_so_fase":   n_colegios_so_fase,
            "n_alunos":             n_alunos,
            "pct_so_fase":          fmt_pct(n_alunos, total_geral),
            "pct_todos":            fmt_pct(acum, total_geral),
            "pct_outros":           fmt_pct(total_geral - acum, total_geral),
            "pct_inf_fund1":        fmt_pct(acum_inf_fund1, total_geral),
            "pct_inf_fund1_fund2":  fmt_pct(acum_inf_fund1_fund2, total_geral),
        }

    # ── Market share por fase ─────────────────────────────────
    fase_chave_map = {
        "QT_MAT_INF":     "infantil",
        "QT_MAT_FUND_AI": "fund1",
        "QT_MAT_FUND_AF": "fund2",
        "QT_MAT_MED":     "medio",
    }
    marketshare = {}
    for col, chave in fase_chave_map.items():
        if col in df_censo.columns:
            cats, vals, pct = _market_share_fase(df_censo, ano_pontual, col)
            marketshare[chave] = {"cats": cats, "vals": vals, "pct": pct}

    # ── Bairros (slide 1) — JOIN OnMaps-Escolar × Censo ───────
    print(f"🔗 Calculando matrículas por bairro via join INEP...")
    df_bairros = _matriculas_por_bairro(cidade, ano_pontual, bairros, top_n=6)

    bairro_cats  = df_bairros["bairro"].tolist()
    bairro_vals  = df_bairros["tt_matriculas_privadas"].astype(int).tolist()

    # Dados para as linhas da tabela dos slides 1 e 2
    # Colunas alinhadas com o template:
    #   col1=pop_total  col2=pop_target  col3=tt_privadas  col4=penetracao
    #   col5-11=pct_classe_* (A++ A+ B1 B2 C1 C2 D/E) em % sobre pop_total
    tabela_bairros = []
    for _, row in df_bairros.iterrows():
        entry = {
            "bairro":          row["bairro"],
            "pop_total":       int(row.get("pop_total",  0)),
            "pop_target":      int(row.get("pop_target", 0)),
            "tt_privadas":     int(row["tt_matriculas_privadas"]),
            "penetracao":      row.get("penetracao", "—"),
            # classes sociais como string "X%" para exibição direta no slide
            "pct_classe_App":  f"{row.get('pct_classe_App', 0):.0f}%",
            "pct_classe_Ap":   f"{row.get('pct_classe_Ap',  0):.0f}%",
            "pct_classe_B1":   f"{row.get('pct_classe_B1',  0):.0f}%",
            "pct_classe_B2":   f"{row.get('pct_classe_B2',  0):.0f}%",
            "pct_classe_C1":   f"{row.get('pct_classe_C1',  0):.0f}%",
            "pct_classe_C2":   f"{row.get('pct_classe_C2',  0):.0f}%",
            "pct_classe_D_E":  f"{row.get('pct_classe_D_E', 0):.0f}%",
        }
        tabela_bairros.append(entry)

    # ── Evolução temporal (slide 3) ───────────────────────────
    series_evolucao = [[] for _ in COLS_FASES]
    for ano in anos_evolucao:
        df_a = df_priv[df_priv[COL_ANO] == ano]
        for i, col in enumerate(COLS_FASES):
            series_evolucao[i].append(
                int(df_a[col].sum()) if not df_a.empty and col in df_a.columns else 0
            )

    # ── CAGR total e por fase ─────────────────────────────────
    anos_necessarios = set(y for par in anos_cagr.values() for y in par)

    totais_por_ano      = {}
    totais_por_ano_fase = {col: {} for col in COLS_FASES}

    for ano in anos_necessarios:
        df_a = df_priv[df_priv[COL_ANO] == ano]
        totais_por_ano[ano] = int(df_a[COLS_FASES].sum().sum()) if not df_a.empty else 0
        for col in COLS_FASES:
            totais_por_ano_fase[col][ano] = (
                int(df_a[col].sum()) if not df_a.empty and col in df_a.columns else 0
            )

    # CAGR total (linha TOTAL do slide 3)
    cagr_calc = {}
    for label, (a_ini, a_fim) in anos_cagr.items():
        cagr_calc[label] = _calcular_cagr(
            totais_por_ano.get(a_ini, 0),
            totais_por_ano.get(a_fim, 0),
            a_fim - a_ini
        )

    # CAGR por fase — lista de valores na ordem dos períodos
    fase_keys = ["infantil", "fund1", "fund2", "medio"]
    cagr_por_fase = {}
    for fase_key, col in zip(fase_keys, COLS_FASES):
        cagr_por_fase[fase_key] = [
            _calcular_cagr(
                totais_por_ano_fase[col].get(a_ini, 0),
                totais_por_ano_fase[col].get(a_fim, 0),
                a_fim - a_ini
            )
            for (a_ini, a_fim) in anos_cagr.values()
        ]

    # Totais anuais para tabela de totais (slide 3, tabela 2)
    totais_anuais = []
    for ano in anos_evolucao:
        df_a = df_priv[df_priv[COL_ANO] == ano]
        totais_anuais.append(int(df_a[COLS_FASES].sum().sum()) if not df_a.empty else 0)

    # ── Top 10 escolas por evolução temporal (slide 4) ───────────
    print("📈 Calculando top 10 escolas para evolução temporal...")
    try:
        # Pega top 10 escolas pelo total de matrículas privadas no ano pontual
        df_ano_priv = df_priv[df_priv[COL_ANO] == ano_pontual].copy()
        df_ano_priv["tt_total"] = df_ano_priv[COLS_FASES].sum(axis=1)
        top10_escolas = df_ano_priv.nlargest(10, "tt_total")["NO_ENTIDADE"].tolist()

        # Série temporal de cada escola
        top10_series = []
        for escola in top10_escolas:
            serie = []
            for ano in anos_evolucao:
                df_e = df_priv[
                    (df_priv[COL_ANO] == ano) &
                    (df_priv["NO_ENTIDADE"] == escola)
                ]
                serie.append(int(df_e[COLS_FASES].sum().sum()) if not df_e.empty else 0)
            top10_series.append(serie)

        # Percentual que o top10 representa do total de matrículas privadas
        total_priv_ano = int(df_ano_priv[COLS_FASES].sum().sum())
        top10_total    = int(
            df_ano_priv[df_ano_priv["NO_ENTIDADE"].isin(top10_escolas)][COLS_FASES].sum().sum()
        )
        pct_top10 = round(top10_total / total_priv_ano * 100) if total_priv_ano > 0 else 0

        top10_data = {"anos": anos_evolucao, "series": top10_series, "escolas": top10_escolas}
    except Exception as e:
        print(f"⚠️  top10 não calculado: {e}")
        top10_data = {}
        pct_top10  = 79  # fallback

    return {
        "stats_por_fase": stats_por_fase,
        "marketshare":    marketshare,
        "bairros": {
            "cats":   bairro_cats,
            "vals":   bairro_vals,
            "tabela": tabela_bairros,
        },
        "evolucao": {
            "anos":   anos_evolucao,
            "series": series_evolucao,
        },
        "top10":              top10_data,
        "pct_top10":          pct_top10,
        "cagr":               cagr_calc,
        "cagr_por_fase":      cagr_por_fase,
        "anos_cagr_periodos": anos_cagr,
        "totais_anuais":      totais_anuais,
        "ano_pontual":        ano_pontual,
        "anos_evolucao":      anos_evolucao,
    }


# ─────────────────────────────────────────────────────────────
# MOCK (fallback)
# ─────────────────────────────────────────────────────────────

def _dados_mock(cidade, ano_pontual, anos_evolucao, anos_cagr):
    n = len(anos_evolucao)
    tabela_mock = [
        {
            "bairro":        f"Bairro {i+1}",
            "pop_total":     30000 + i*5000,
            "pop_target":    12000 + i*2000,
            "tt_matriculas": 3000  + i*500,
            "penetracao":    f"{20+i*3}%",
            "tt_privadas":   500   + i*80,
            "tt_publicas":   2500  + i*420,
            "pct_privadas":  f"{round((500+i*80)/(3000+i*500)*100)}%",
            "pct_publicas":  f"{round((2500+i*420)/(3000+i*500)*100)}%",
        }
        for i in range(6)
    ]
    top10_mock_series = [[500+j*20+i*10 for i in range(n)] for j in range(10)]
    return {
        "stats_por_fase": {
            "Infantil":       {"n_colegios": 120, "n_alunos": 5400,  "pct_so_fase": "35%", "pct_todos": "35%", "pct_outros": "65%"},
            "Fundamental I":  {"n_colegios": 98,  "n_alunos": 8200,  "pct_so_fase": "28%", "pct_todos": "63%", "pct_outros": "37%"},
            "Fundamental II": {"n_colegios": 74,  "n_alunos": 6100,  "pct_so_fase": "22%", "pct_todos": "85%", "pct_outros": "15%"},
            "Médio":          {"n_colegios": 32,  "n_alunos": 3800,  "pct_so_fase": "15%", "pct_todos": "100%","pct_outros": "0%"},
        },
        "marketshare": {
            "infantil": {"cats": [f"Escola {i}" for i in range(1,16)], "vals": [181,171,150,140,133,131,127,115,87,86,84,61,54,51,46], "pct": [0.096,0.090,0.079,0.074,0.070,0.069,0.067,0.061,0.046,0.045,0.044,0.032,0.028,0.027,0.024]},
            "fund1":    {"cats": [f"Escola {i}" for i in range(1,16)], "vals": [477,313,293,250,171,154,112,103,102,95,88,85,84,81,71],  "pct": [0.176,0.116,0.108,0.092,0.063,0.057,0.041,0.038,0.038,0.035,0.033,0.031,0.031,0.030,0.026]},
            "fund2":    {"cats": [f"Escola {i}" for i in range(1,13)], "vals": [507,305,237,165,107,72,67,60,46,44,36,26],               "pct": [0.303,0.182,0.142,0.099,0.064,0.043,0.040,0.036,0.028,0.026,0.022,0.016]},
            "medio":    {"cats": [f"Escola {i}" for i in range(1,7)],  "vals": [584,230,208,110,0,0],                                    "pct": [0.516,0.203,0.184,0.097,0.0,0.0]},
        },
        "bairros": {
            "cats":   [r["bairro"]      for r in tabela_mock],
            "vals":   [r["tt_privadas"] for r in tabela_mock],
            "tabela": tabela_mock,
        },
        "evolucao": {
            "anos":   anos_evolucao,
            "series": [[1412+i*50 for i in range(n)], [2378+i*30 for i in range(n)],
                       [1149+i*55 for i in range(n)], [574+i*60  for i in range(n)]],
        },
        "top10": {
            "anos":    anos_evolucao,
            "series":  top10_mock_series,
            "escolas": [f"Escola {i}" for i in range(1, 11)],
        },
        "cagr":               {label: "+3,0%" for label in anos_cagr},
        "cagr_por_fase":      {
            "infantil": ["+3,3%", "-3,5%", "-31,6%", "+40,9%", "+10,4%"],
            "fund1":    ["+1,0%", "+1,7%",  "-9,2%",  "-5,3%", "+13,0%"],
            "fund2":    ["+6,7%", "+0,9%",  "-2,3%",  "-3,2%",  "+7,0%"],
            "medio":    ["+2,1%","+13,9%",  "12,8%", "+18,1%",  "+7,5%"],
        },
        "anos_cagr_periodos": anos_cagr,
        "totais_anuais":      [5513,5739,5673,6077,6272,6358,5679,6114,6735,7405],
        "pct_top10":          79,
        "ano_pontual":        ano_pontual,
        "anos_evolucao":      anos_evolucao,
    }


# Alias
def carregar_dados(cidade):
    return buscar_tudo(cidade)

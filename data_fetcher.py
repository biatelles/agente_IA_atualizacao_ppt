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
    Lê Microareas.csv e filtra pelo município.
    Retorna DataFrame com dados demográficos por microárea.
    Os valores numéricos estão em formato brasileiro (1.234,56).
    """
    df = pd.read_excel(MICROAREAS_PATH)
    df.columns = df.columns.str.strip()
    df[MICROAREAS_COL_MUNICIPIO] = df[MICROAREAS_COL_MUNICIPIO].astype(str).str.strip()
    cidade_norm = _normalizar_str(cidade)
    mask = df[MICROAREAS_COL_MUNICIPIO].apply(_normalizar_str) == cidade_norm
    df = df[mask].copy()

    # Converte todas as colunas numéricas do formato BR para float
    cols_numericas = [
        MICROAREAS_COL_POPULACAO,
        MICROAREAS_COL_POP_ATE9, MICROAREAS_COL_POP_10_14, MICROAREAS_COL_POP_15_19,
        MICROAREAS_COL_RENDA_MEDIA,
    ] + [MICROAREAS_COL_CLASSE_PREFIX + c for c in MICROAREAS_CLASSES]       + [MICROAREAS_COL_POP_CLASSE_PREFIX + c for c in MICROAREAS_POP_CLASSES]

    for col in cols_numericas:
        if col in df.columns:
            df[col] = _br_to_float(df[col])

    return df


# ─────────────────────────────────────────────────────────────
# JOIN: base_completa × Censo → matrículas privadas por microárea
#       + Microáreas → dados demográficos
# ─────────────────────────────────────────────────────────────

def _matriculas_por_bairro(cidade, ano, bairros="automatico", top_n=6):
    """
    Retorna DataFrame com matrículas privadas e dados demográficos por microárea.
    Chave de join:
      base_completa.Código INEP ←→ Censo.CO_ENTIDADE  (via INEP normalizado)
      base_completa.Microárea   ←→ Microáreas.Microáreas  (via chave composta Microárea+Município+Estado)
    """
    # 1. Censo: escolas privadas do município/ano
    df_censo = _ler_censo(cidade)
    df_censo_ano = df_censo[
        (df_censo[COL_ANO] == ano) &
        (df_censo[COL_DEPENDENCIA] == VAL_PRIVADA)
    ].copy()
    df_censo_ano["tt_privadas"] = df_censo_ano[COLS_FASES].sum(axis=1)
    df_censo_ano["_inep"] = _normalizar_inep(df_censo_ano["CO_ENTIDADE"])

    # Censo total (públicas + privadas)
    df_censo_total = df_censo[df_censo[COL_ANO] == ano].copy()
    df_censo_total["tt_total"] = df_censo_total[COLS_FASES].sum(axis=1)
    df_censo_total["_inep"] = _normalizar_inep(df_censo_total["CO_ENTIDADE"])

    # 2. base_completa: INEP → Microárea
    print(f"🔗 Calculando matrículas por microárea via join INEP...")
    df_bc = _ler_base_completa(cidade)
    if df_bc.empty:
        raise ValueError(f"Cidade '{cidade}' não encontrada na base_completa.")
    df_bc["_inep"] = _normalizar_inep(df_bc[BASE_COMPLETA_COL_INEP])

    # 3. Join: base_completa × Censo (privadas)
    df_join = df_bc.merge(
        df_censo_ano[["_inep", "tt_privadas"] + COLS_FASES],
        on="_inep", how="inner"
    )
    if df_join.empty:
        raise ValueError(
            f"Join base_completa × Censo retornou vazio para {cidade}/{ano}. "
            "Verifique se os códigos INEP batem."
        )

    # 4. Agrega por microárea — matrículas privadas
    col_ma = BASE_COMPLETA_COL_MICROAREA
    df_ma_priv = (
        df_join.groupby(col_ma)["tt_privadas"]
        .sum().reset_index()
        .rename(columns={col_ma: "bairro", "tt_privadas": "tt_matriculas_privadas"})
    )

    # Filtra microáreas explícitas se fornecido
    if bairros != "automatico" and isinstance(bairros, list):
        df_ma_priv = df_ma_priv[df_ma_priv["bairro"].isin(bairros)]

    # Top N por matrículas privadas
    df_ma_priv = df_ma_priv.sort_values("tt_matriculas_privadas", ascending=False).head(top_n)

    # 5. Join: base_completa × Censo total e públicas → matrículas por microárea
    try:
        # Total (todas dependências)
        df_join_total = df_bc.merge(
            df_censo_total[["_inep", "tt_total"]],
            on="_inep", how="inner"
        )
        df_ma_total = (
            df_join_total.groupby(col_ma)["tt_total"]
            .sum().reset_index()
            .rename(columns={col_ma: "bairro", "tt_total": "tt_matriculas_total"})
        )
        df_ma_priv = df_ma_priv.merge(df_ma_total, on="bairro", how="left")

        # Públicas (Federal + Estadual + Municipal)
        df_censo_pub = df_censo[
            (df_censo[COL_ANO] == ano) &
            (df_censo[COL_DEPENDENCIA] != VAL_PRIVADA)
        ].copy()
        df_censo_pub["tt_publicas_escola"] = df_censo_pub[COLS_FASES].sum(axis=1)
        df_censo_pub["_inep"] = _normalizar_inep(df_censo_pub["CO_ENTIDADE"])

        df_join_pub = df_bc.merge(
            df_censo_pub[["_inep", "tt_publicas_escola"]],
            on="_inep", how="inner"
        )
        df_ma_pub = (
            df_join_pub.groupby(col_ma)["tt_publicas_escola"]
            .sum().reset_index()
            .rename(columns={col_ma: "bairro", "tt_publicas_escola": "tt_matriculas_publicas"})
        )
        df_ma_priv = df_ma_priv.merge(df_ma_pub, on="bairro", how="left")
        df_ma_priv["tt_matriculas_publicas"] = df_ma_priv["tt_matriculas_publicas"].fillna(0)
    except Exception as e:
        print(f"⚠️  Matrículas totais/públicas: {e}")
        df_ma_priv["tt_matriculas_total"]   = df_ma_priv["tt_matriculas_privadas"]
        df_ma_priv["tt_matriculas_publicas"] = 0

    # 6. Enriquece com dados demográficos das Microáreas
    try:
        df_demo = _ler_microareas(cidade)
        if not df_demo.empty:
            # Calcula pop_target = soma das faixas etárias até 19 anos
            # (conversão BR→float já feita em _ler_microareas)
            cols_ate19 = [MICROAREAS_COL_POP_ATE9, MICROAREAS_COL_POP_10_14, MICROAREAS_COL_POP_15_19]
            cols_existentes = [c for c in cols_ate19 if c in df_demo.columns]
            df_demo["pop_target"] = df_demo[cols_existentes].sum(axis=1) if cols_existentes else 0

            # Calcula percentuais de população por classe social
            # Colunas: "População por Faixa de Renda vs Faixa Etária - A++" etc.
            # Total = soma de todas as classes (não usar coluna "População" pois pode divergir)
            POP_PREFIX = MICROAREAS_COL_POP_CLASSE_PREFIX
            pop_classes_map = {
                "A++": "pct_classe_App",
                "A+":  "pct_classe_Ap",
                "B1":  "pct_classe_B1",
                "B2":  "pct_classe_B2",
                "C1":  "pct_classe_C1",
                "C2":  "pct_classe_C2",
                "D":   "pct_classe_D_raw",
                "E":   "pct_classe_E_raw",
            }
            cols_pop_classe = [
                POP_PREFIX + c for c in pop_classes_map
                if POP_PREFIX + c in df_demo.columns
            ]
            if cols_pop_classe:
                df_demo["_pop_classe_total"] = df_demo[cols_pop_classe].sum(axis=1)
                for classe, col_pct in pop_classes_map.items():
                    col_bruta = POP_PREFIX + classe
                    if col_bruta in df_demo.columns:
                        df_demo[col_pct] = df_demo.apply(
                            lambda r: fmt_pct(r[col_bruta], r["_pop_classe_total"]), axis=1
                        )
                # D/E = soma D + E
                col_d = POP_PREFIX + "D"
                col_e = POP_PREFIX + "E"
                if col_d in df_demo.columns and col_e in df_demo.columns:
                    df_demo["pct_classe_D_E"] = df_demo.apply(
                        lambda r: fmt_pct(r[col_d] + r[col_e], r["_pop_classe_total"]), axis=1
                    )
                else:
                    df_demo["pct_classe_D_E"] = df_demo.get("pct_classe_D_raw", "—")

            # Seleciona colunas para merge
            cols_sel = [MICROAREAS_COL_MICROAREA, MICROAREAS_COL_POPULACAO, "pop_target"]
            cols_pct = [c for c in ["pct_classe_App","pct_classe_Ap","pct_classe_B1","pct_classe_B2",
                                    "pct_classe_C1","pct_classe_C2","pct_classe_D_E"] if c in df_demo.columns]
            cols_sel += cols_pct

            df_demo_sel = df_demo[cols_sel].copy()
            df_demo_sel = df_demo_sel.rename(columns={
                MICROAREAS_COL_MICROAREA: "bairro",
                MICROAREAS_COL_POPULACAO: "pop_total",
            })
            # Garante mesmo tipo nas chaves antes do merge
            df_ma_priv["bairro"]  = df_ma_priv["bairro"].astype(str).str.strip()
            df_demo_sel["bairro"] = df_demo_sel["bairro"].astype(str).str.strip()
            df_ma_priv = df_ma_priv.merge(df_demo_sel, on="bairro", how="left")
        else:
            df_ma_priv["pop_total"]  = 0
            df_ma_priv["pop_target"] = 0
    except Exception as e:
        print(f"⚠️  Dados demográficos não carregados: {e}")
        df_ma_priv["pop_total"]  = 0
        df_ma_priv["pop_target"] = 0

    # 7. Garante colunas e calcula penetração
    for col_fill in ["pop_total", "pop_target", "tt_matriculas_total", "tt_matriculas_publicas"]:
        if col_fill not in df_ma_priv.columns:
            df_ma_priv[col_fill] = 0
        df_ma_priv[col_fill] = pd.to_numeric(df_ma_priv[col_fill], errors="coerce").fillna(0)
    # penetração = (privadas + públicas) / pop_target
    df_ma_priv["_tt_tot_calc"] = df_ma_priv["tt_matriculas_privadas"] + df_ma_priv["tt_matriculas_publicas"]
    df_ma_priv["penetracao"] = df_ma_priv.apply(
        lambda r: fmt_pct(r["_tt_tot_calc"], r["pop_target"]), axis=1
    )

    return df_ma_priv.reset_index(drop=True)


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
            stats_por_fase[fase_label] = {"n_colegios":0,"n_colegios_so_fase":0,
                "n_alunos":0,"pct_so_fase":"0%","pct_todos":"0%","pct_outros":"100%",
                "pct_inf_fund1":"0%","pct_inf_fund1_fund2":"0%"}
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
        acum_inf_fund1 = int(df_ano[[c for c in cols_inf_fund1 if c in df_ano.columns]].sum().sum())
        acum_inf_fund1_fund2 = int(df_ano[[c for c in cols_inf_fund1_fund2 if c in df_ano.columns]].sum().sum())

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
    tabela_bairros = []
    for _, row in df_bairros.iterrows():
        tt_priv = int(row["tt_matriculas_privadas"])
        tt_pub  = int(row.get("tt_matriculas_publicas", 0))
        tt_tot  = tt_priv + tt_pub
        entry = {
            "bairro":       row["bairro"],
            "pop_total":    int(row.get("pop_total", 0)),
            "pop_target":   int(row.get("pop_target", 0)),
            "tt_matriculas":tt_tot,
            "penetracao":   row.get("penetracao", "—"),
            "tt_privadas":  tt_priv,
            "tt_publicas":  tt_pub,
            "pct_privadas": fmt_pct(tt_priv, tt_tot),
            "pct_publicas": fmt_pct(tt_pub,  tt_tot),
        }
        # Classes sociais (se disponíveis da base Microáreas)
        for col_pct in ["pct_classe_App","pct_classe_Ap","pct_classe_B1","pct_classe_B2",
                         "pct_classe_C1","pct_classe_C2","pct_classe_D_E"]:
            if col_pct in row.index:
                entry[col_pct] = row.get(col_pct, "—")
        tabela_bairros.append(entry)

    # ── Evolução temporal (slide 3) ───────────────────────────
    series_evolucao = [[] for _ in COLS_FASES]
    for ano in anos_evolucao:
        df_a = df_priv[df_priv[COL_ANO] == ano]
        for i, col in enumerate(COLS_FASES):
            series_evolucao[i].append(int(df_a[col].sum()) if not df_a.empty and col in df_a.columns else 0)

    # ── CAGR total e por fase ─────────────────────────────────
    anos_necessarios = set(y for par in anos_cagr.values() for y in par)

    totais_por_ano      = {}
    totais_por_ano_fase = {col: {} for col in COLS_FASES}

    for ano in anos_necessarios:
        df_a = df_priv[df_priv[COL_ANO] == ano]
        totais_por_ano[ano] = int(df_a[COLS_FASES].sum().sum()) if not df_a.empty else 0
        for col in COLS_FASES:
            totais_por_ano_fase[col][ano] = int(df_a[col].sum()) if not df_a.empty and col in df_a.columns else 0

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
        top10_escolas = (
            df_ano_priv.nlargest(10, "tt_total")["NO_ENTIDADE"].tolist()
        )
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
        top10_total    = int(df_ano_priv[df_ano_priv["NO_ENTIDADE"].isin(top10_escolas)][COLS_FASES].sum().sum())
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
            "bairro":       f"Bairro {i+1}",
            "pop_total":    30000 + i*5000,
            "pop_target":   12000 + i*2000,
            "tt_matriculas":3000  + i*500,
            "penetracao":   f"{20+i*3}%",
            "tt_privadas":  500   + i*80,
            "tt_publicas":  2500  + i*420,
            "pct_privadas": f"{round((500+i*80)/(3000+i*500)*100)}%",
            "pct_publicas": f"{round((2500+i*420)/(3000+i*500)*100)}%",
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
        "ano_pontual":   ano_pontual,
        "anos_evolucao": anos_evolucao,
    }


# Alias
def carregar_dados(cidade):
    return buscar_tudo(cidade)

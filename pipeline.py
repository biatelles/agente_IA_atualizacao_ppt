"""
pipeline.py — versão final com todos os bugs corrigidos

Correções:
1. CORRUPÇÃO: valores substituídos com < (ex: "Pop. <19 anos") quebravam o XML.
   Corrigido com xml.sax.saxutils.escape() em todos os valores de subs.
2. CAGR: valores calculados por fase são inseridos diretamente nas células
   da tabela, sobrescrevendo os valores fixos do template (Barra Mansa).
   Cabeçalhos das colunas mostram os anos reais (ex: "2010-2025").
3. Tabela 2 do slide 3 (totais anuais) é atualizada com os valores reais.
4. Slide 4: labels das séries do chart9 usam nomes das escolas top10.
5. Slides 5-8: eixo X mostra nomes reais das escolas.
6. ZIP: preserva compress_type e date_time originais para evitar corrupção.
"""

import zipfile
from xml.sax.saxutils import escape as xml_escape
from lxml import etree

NS_C = "http://schemas.openxmlformats.org/drawingml/2006/chart"
NS_A = "http://schemas.openxmlformats.org/drawingml/2006/main"
NS = {"c": NS_C, "a": NS_A}


def ctag(x):
    return f"{{{NS_C}}}{x}"


def fmt_num(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def safe(value):
    """Escapa < > & para inserção segura em XML."""
    return xml_escape(str(value))


# ─────────────────────────────────────────────────────
# Atualizar cache de dados (categorias ou valores)
# ─────────────────────────────────────────────────────
def update_cache(cache, values):
    if cache is None:
        return
    pt_count = cache.find("c:ptCount", NS)
    if pt_count is not None:
        pt_count.set("val", str(len(values)))
    for old in list(cache.findall("c:pt", NS)):
        cache.remove(old)
    for i, v in enumerate(values):
        pt = etree.SubElement(cache, ctag("pt"))
        pt.set("idx", str(i))
        val_el = etree.SubElement(pt, ctag("v"))
        val_el.text = str(v)


# ─────────────────────────────────────────────────────
# Atualizar gráfico de barras (ou combo bar+line)
# ─────────────────────────────────────────────────────
def atualizar_barras(xml_bytes, cats, vals, pct_vals=None):
    tree = etree.fromstring(xml_bytes)
    all_sers = tree.findall(".//c:ser", NS)
    if not all_sers:
        return xml_bytes
    for i, ser in enumerate(all_sers):
        cat_cache = ser.find(".//c:cat//c:strCache", NS)
        if cat_cache is None:
            cat_cache = ser.find(".//c:cat//c:numCache", NS)
        val_cache = ser.find(".//c:val//c:numCache", NS)
        update_cache(cat_cache, cats)
        if i == 0:
            update_cache(val_cache, vals)
        elif pct_vals is not None:
            update_cache(val_cache, pct_vals)
        else:
            update_cache(val_cache, vals)
    return etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────────────
# Atualizar gráfico de linhas multi-série
# ─────────────────────────────────────────────────────
def atualizar_linhas(xml_bytes, anos, series_data, series_labels=None):
    tree = etree.fromstring(xml_bytes)
    all_sers = tree.findall(".//c:ser", NS)
    for i, ser in enumerate(all_sers):
        if series_labels and i < len(series_labels):
            tx_v = ser.find(".//c:tx//c:v", NS)
            if tx_v is not None:
                tx_v.text = str(series_labels[i])
        cat_cache = ser.find(".//c:cat//c:strCache", NS)
        if cat_cache is None:
            cat_cache = ser.find(".//c:cat//c:numCache", NS)
        val_cache = ser.find(".//c:val//c:numCache", NS)
        update_cache(cat_cache, anos)
        if i < len(series_data):
            update_cache(val_cache, series_data[i])
    return etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────────────
# Substituir texto nos slides
# ─────────────────────────────────────────────────────
def _merge_runs_in_paragraph(para):
    runs = para.findall(f"{{{NS_A}}}r")
    if len(runs) <= 1:
        return
    # Não mesclar se houver <a:br> (quebra de linha) — destruiria a formatação
    if para.find(f"{{{NS_A}}}br") is not None:
        # Substitui apenas no run que contém o placeholder
        for run in runs:
            t_el = run.find(f"{{{NS_A}}}t")
            if t_el is not None and t_el.text and "{{" in t_el.text:
                return  # placeholder já está num run isolado, não precisa mesclar
        return
    full_text = "".join(
        (r.find(f"{{{NS_A}}}t").text or "")
        for r in runs
        if r.find(f"{{{NS_A}}}t") is not None
    )
    first_rpr = runs[0].find(f"{{{NS_A}}}rPr")
    for r in runs:
        para.remove(r)
    new_run = etree.SubElement(para, f"{{{NS_A}}}r")
    if first_rpr is not None:
        new_run.insert(0, first_rpr)
    t_el = etree.SubElement(new_run, f"{{{NS_A}}}t")
    t_el.text = full_text


def substituir_texto(xml_bytes, subs):
    # Merge fragmentos de runs
    try:
        tree = etree.fromstring(xml_bytes)
        for para in tree.iter(f"{{{NS_A}}}p"):
            text = "".join(
                (r.find(f"{{{NS_A}}}t").text or "")
                for r in para.findall(f"{{{NS_A}}}r")
                if r.find(f"{{{NS_A}}}t") is not None
            )
            if "{{" in text and "}}" in text:
                _merge_runs_in_paragraph(para)
        xml_bytes = etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)
    except Exception:
        pass

    txt = xml_bytes.decode("utf-8")

    # Replace normal placeholders (valores já escaped com safe())
    for k, v in subs.items():
        txt = txt.replace(k, str(v))

    # Replace placeholders com < > que o lxml serializa como &lt; &gt;
    for k, v in subs.items():
        k_enc = k.replace("<", "&lt;").replace(">", "&gt;")
        if k_enc != k:
            txt = txt.replace(k_enc, str(v))

    return txt.encode("utf-8")


# ─────────────────────────────────────────────────────
# Substituir valores fixos na tabela CAGR (slide 3)
# As células têm texto fixo (ex: "+3,3%"), não placeholders
# Fazemos substituição lxml direta nas células da tabela
# ─────────────────────────────────────────────────────
def atualizar_tabela_cagr(xml_bytes, dados):
    """
    Substitui os valores das células da tabela CAGR do slide 3.
    
    Estrutura da tabela:
      Row 0: cabeçalhos (ANO1-ANO10 etc.) → já cobertos por subs
      Row 1: Infantil  + 5 valores de CAGR
      Row 2: Fund. I   + 5 valores de CAGR
      Row 3: Fund. II  + 5 valores de CAGR
      Row 4: Médio     + 5 valores de CAGR
      Row 5: TOTAL     + 5 valores de CAGR total
    
    Tabela 2 (totais anuais): 1 linha com 10 valores
    """
    cagr_por_fase = dados.get("cagr_por_fase", {})
    cagr_total    = dados.get("cagr", {})
    totais_anuais = dados.get("totais_anuais", [])

    tree = etree.fromstring(xml_bytes)
    tables = tree.findall(f".//{{{NS_A}}}tbl")

    if not tables:
        return xml_bytes

    # ── Tabela 0: CAGR por fase ────────────────────────
    tbl0 = tables[0]
    rows = tbl0.findall(f"{{{NS_A}}}tr")

    # Ordem das linhas: row1=infantil, row2=fund1, row3=fund2, row4=medio, row5=total
    fase_ordem = ["infantil", "fund1", "fund2", "medio"]
    cagr_total_vals = list(cagr_total.values())

    COR_POSITIVO = "006100"
    COR_NEGATIVO = "9C0006"
    COR_NEUTRO   = "404040"

    def _aplicar_cor_cagr(cell, valor_str):
        """Atualiza texto e cor do rPr da célula conforme sinal do valor."""
        t_els = list(cell.iter(f"{{{NS_A}}}t"))
        if t_els:
            t_els[0].text = str(valor_str)
            for t in t_els[1:]:
                t.text = ""
        # Determina cor pelo sinal
        s = str(valor_str).strip()
        if s.startswith("+"):
            cor = COR_POSITIVO
        elif s.startswith("-"):
            cor = COR_NEGATIVO
        else:
            cor = COR_NEUTRO
        # Atualiza srgbClr dentro do rPr da célula
        for rpr in cell.iter(f"{{{NS_A}}}rPr"):
            clr = rpr.find(f".//{{{NS_A}}}srgbClr")
            if clr is not None:
                clr.set("val", cor)
            else:
                # Cria solidFill com srgbClr se não existir
                solid = etree.SubElement(rpr, f"{{{NS_A}}}solidFill")
                srgb  = etree.SubElement(solid, f"{{{NS_A}}}srgbClr")
                srgb.set("val", cor)

    for row_idx, row in enumerate(rows):
        cells = row.findall(f"{{{NS_A}}}tc")

        if row_idx == 0:
            continue  # cabeçalho

        if 1 <= row_idx <= 4:
            fase_key = fase_ordem[row_idx - 1]
            vals = cagr_por_fase.get(fase_key, [])
        elif row_idx == 5:
            vals = cagr_total_vals
        else:
            continue

        for col_idx, cell in enumerate(cells):
            if col_idx == 0:
                continue  # label da fase
            val_idx = col_idx - 1
            if val_idx >= len(vals):
                continue
            _aplicar_cor_cagr(cell, vals[val_idx])

    # ── Tabela 1: totais anuais ────────────────────────
    if len(tables) > 1 and totais_anuais:
        tbl1 = tables[1]
        rows1 = tbl1.findall(f"{{{NS_A}}}tr")
        if rows1:
            cells1 = rows1[0].findall(f"{{{NS_A}}}tc")
            for c_idx, cell in enumerate(cells1):
                if c_idx < len(totais_anuais):
                    t_els = list(cell.iter(f"{{{NS_A}}}t"))
                    if t_els:
                        t_els[0].text = fmt_num(totais_anuais[c_idx])
                        for t in t_els[1:]:
                            t.text = ""

    return etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────────────
# Montar dicionário de substituições de texto
# ─────────────────────────────────────────────────────
def _periodo_label(anos_cagr, idx):
    pairs = list(anos_cagr.items())
    if idx < len(pairs):
        _, (a_ini, a_fim) = pairs[idx]
        return f"{a_ini}-{a_fim}"
    return ""


def montar_subs(cidade, dados):
    stats     = dados["stats_por_fase"]
    inf       = stats.get("Infantil", {})
    fund1     = stats.get("Fundamental I", {})
    fund2     = stats.get("Fundamental II", {})
    medio     = stats.get("Médio", {})
    anos_cagr = dados.get("anos_cagr_periodos", {})
    anos_e    = dados.get("anos_evolucao", [])

    subs = {
        "{{CIDADE}}": safe(cidade),

        # ── Slides 5-8 ─────────────────────────────────
        # Slides 5-8: subtítulos
        # {{NUM_COLEGIOS_X}} = colégios que ofertam SOMENTE aquela fase
        "{{NUM_COLEGIOS_INF}}":    safe(str(inf.get("n_colegios_so_fase", ""))),
        "{{NUM_COLEGIOS_FUND1}}":  safe(str(fund1.get("n_colegios_so_fase", ""))),
        "{{NUM_COLEGIOS_FUND2}}":  safe(str(fund2.get("n_colegios_so_fase", ""))),
        "{{NUM_COLEGIOS_MEDIO}}":  safe(str(medio.get("n_colegios_so_fase", ""))),
        # {{NUM_ALUNOS_X}} = total de alunos naquela fase (privadas)
        "{{NUM_ALUNOS_INF}}":      safe(fmt_num(inf.get("n_alunos", ""))),
        "{{NUM_ALUNOS_FUND1}}":    safe(fmt_num(fund1.get("n_alunos", ""))),
        "{{NUM_ALUNOS_FUND2}}":    safe(fmt_num(fund1.get("n_alunos", ""))),   # slide6 usa FUND2 mas refere-se ao FUND1
        "{{NUM_ALUNOS _FUND2}}":   safe(fmt_num(fund2.get("n_alunos", ""))),   # typo no template slide7
        "{{NUM_ALUNOS_MEDIO}}":    safe(fmt_num(medio.get("n_alunos", ""))),
        # Slide 5: {{PCT_INF}} = % somente INF / {{PCT_INF_FUND1}} = % INF+FUND1 acumulado
        # Atenção: apenas {{PCT_OUTROS}} tem % literal no template após o placeholder
        # Os demais não têm % literal — manter o % no valor
        "{{PCT_INF}}":             safe(inf.get("pct_so_fase", "")),
        # Slide5 tem "{{PCT_INF_FUND1}}%" (com % literal) → substituímos o par completo sem duplicar
        "{{PCT_INF_FUND1}}%":      safe(inf.get("pct_inf_fund1", "")),   # slide5: remove o % do template
        "{{PCT_INF_FUND1}}":       safe(inf.get("pct_inf_fund1", "")),   # slide6: sem % literal
        "{{PCT_INF_FUND1_FUND2}}": safe(fund2.get("pct_inf_fund1_fund2", "")),
        "{{PCT_TODOS}}":           safe(medio.get("pct_todos", "")),
        "{{PCT_OUTROS}}":          safe(inf.get("pct_outros", "").rstrip("%")),

        # ── Slide 3: labels das fases ───────────────────
        "{{STATS_INFANTIL}}": "Infantil",
        "{{STATS_FUND1}}":    "Fund. I",
        "{{STATS_FUND2}}":    "Fund. II",
        "{{STATS_MEDIO}}":    "M\u00e9dio",

        # ── Slide 3: cabeçalhos das colunas CAGR ───────
        # Mostra os anos reais: "2010-2025", "2020-2021" etc.
        "{{ANO1-ANO10}}": _periodo_label(anos_cagr, 0),
        "{{ANO5-ANO7}}":  _periodo_label(anos_cagr, 1),
        "{{ANO7-ANO8}}":  _periodo_label(anos_cagr, 2),
        "{{ANO8-ANO9}}":  _periodo_label(anos_cagr, 3),
        "{{ANO10-ANO9}}": _periodo_label(anos_cagr, 4),

        # ── Slide 2: cabeçalhos da tabela ───────────────
        "{{POPULACAO_TOTAL}}":   "Pop. Total",
        "{{POPULACAO<19}}":      safe("Pop. <19 anos"),
        "{{POPULACAO<19ANOS}}":  safe("Pop. <19 anos"),
        "{{TOTAL_MATRICULAS_PUBLICAS_E_PARTICULARES}}":                   "TT Matr\u00edculas",
        "{{PENETRACAO_MERCADO(TOTAL_MATRICULAS/POPULACAO_TARGET_EM_%)}}": "Penetra\u00e7\u00e3o",
        "{{TOTAL_MATRICULAS}}":                  "TT Matr\u00edculas",
        "{{TOTAL_MATRICULAS/POPULACAO<19ANOS}}": safe("Matr./Pop.<19"),
        "{{TOTAL_MATRIUCLAS_PARTICULARES}}":     "TT Particulares",
        "{{TOTAL_MATRICULAS_PUBLICAS}}":         "TT P\u00fablicas",
        "{{PROPOCAO_MATRICULAS_PARTICULARES}}":  "% Particulares",
        "{{PROPORCAO_MATRICULAS_PUBLICAS}}":     "% P\u00fablicas",
        "{{A++}}": "A++", "{{A+}}": "A+",
        "{{B1}}": "B1", "{{B2}}": "B2",
        "{{C1}}": "C1", "{{C2}}": "C2",
        "{{DE}}": "D/E",
    }

    # Slide 4: subtítulo — substitui texto fixo do template
    # "79% das matrículas particulares de Barra Mansa" → valores reais
    pct_top10 = str(dados.get("pct_top10", "79"))
    subs["{{PCT_TOP10}}"]    = safe(pct_top10)
    # Substitui os valores fixos hardcoded no template diretamente
    subs["79% das matrículas particulares de Barra Mansa"] = safe(
        f"{pct_top10}% das matrículas particulares de {cidade}"
    )

    # Anos: {{ANO1}}..{{ANO10}}
    for i, ano in enumerate(anos_e, start=1):
        subs[f"{{{{ANO{i}}}}}"] = str(ano)

    # Bairros
    tabela = dados.get("bairros", {}).get("tabela", [])
    for i, row in enumerate(tabela[:6], start=1):
        n = str(i)
        subs[f"{{{{BAIRRO{n}}}}}"]       = safe(row.get("bairro", ""))
        subs[f"{{{{POP_TOTAL_B{n}}}}}"]  = safe(fmt_num(row.get("pop_total", "")))
        subs[f"{{{{POP_TARGET_B{n}}}}}"] = safe(fmt_num(row.get("pop_target", "")))
        subs[f"{{{{TT_MAT_B{n}}}}}"]     = safe(fmt_num(row.get("tt_matriculas", "")))
        subs[f"{{{{PENETRACAO_B{n}}}}}"] = safe(row.get("penetracao", ""))
        subs[f"{{{{TT_PRIV_B{n}}}}}"]    = safe(fmt_num(row.get("tt_privadas", "")))
        subs[f"{{{{TT_PART_B{n}}}}}"]    = safe(fmt_num(row.get("tt_privadas", "")))
        subs[f"{{{{TT_PUB_B{n}}}}}"]     = safe(fmt_num(row.get("tt_publicas", "")))
        subs[f"{{{{PCT_PART_B{n}}}}}"]   = safe(row.get("pct_privadas", ""))
        subs[f"{{{{PCT_PUB_B{n}}}}}"]    = safe(row.get("pct_publicas", ""))

    # ── Colégios: {{COLEGIO1}}..{{COLEGIO15}} (eixo X dos charts 10-13) ──
    for fase_key in ['infantil', 'fund1', 'fund2', 'medio']:
        ms = dados.get('marketshare', {}).get(fase_key, {})
        cats = ms.get('cats', [])
        for i, nome in enumerate(cats, start=1):
            subs[f'{{{{COLEGIO{i}}}}}'] = safe(nome)

    return subs


# ─────────────────────────────────────────────────────
# Mapeamento slide → chart
# ─────────────────────────────────────────────────────
CHART_MAP = {
    "ppt/charts/chart1.xml":  ("bairros",  "dist"),
    "ppt/charts/chart8.xml":  ("evolucao", "linhas"),
    "ppt/charts/chart9.xml":  ("top10",    "linhas"),
    "ppt/charts/chart10.xml": ("infantil", "barras"),
    "ppt/charts/chart11.xml": ("fund1",    "barras"),
    "ppt/charts/chart12.xml": ("fund2",    "barras"),
    "ppt/charts/chart13.xml": ("medio",    "barras"),
}


# ─────────────────────────────────────────────────────
# Atualizar tabela de microáreas do slide 1
# Células têm valores fixos do template — substituição via lxml
# Colunas: [nome_microarea, pop_total, pop_target, tt_matriculas, penetracao]
# ─────────────────────────────────────────────────────
def atualizar_tabela_microareas(xml_bytes, dados):
    tabela = dados.get("bairros", {}).get("tabela", [])
    if not tabela:
        return xml_bytes

    tree = etree.fromstring(xml_bytes)
    tables = tree.findall(f".//{{{NS_A}}}tbl")
    if not tables:
        return xml_bytes

    tbl = tables[0]  # primeira tabela = dados por microárea
    rows = tbl.findall(f"{{{NS_A}}}tr")

    for r_idx, row in enumerate(rows):
        if r_idx >= len(tabela):
            break
        ma = tabela[r_idx]
        cells = row.findall(f"{{{NS_A}}}tc")

        # col 0: nome da microárea — já coberto por {{BAIRRO1}} etc. em substituir_texto
        # col 1: pop_total
        # col 2: pop_target (<19 anos)
        # col 3: tt_matriculas (total público + privado)
        # col 4: penetração
        valores = [
            None,  # col 0: não tocar — substituído por subs
            fmt_num(ma.get("pop_total", 0)),
            fmt_num(ma.get("pop_target", 0)),
            fmt_num(ma.get("tt_matriculas", 0)),
            ma.get("penetracao", "—"),
        ]

        for c_idx, cell in enumerate(cells):
            if c_idx == 0 or valores[c_idx] is None:
                continue
            t_els = list(cell.iter(f"{{{NS_A}}}t"))
            if t_els:
                t_els[0].text = str(valores[c_idx])
                for t in t_els[1:]:
                    t.text = ""

    return etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────────────
# Atualizar tabela do slide 2 (caracterização dos mercados)
# Colunas por microárea:
#   0: nome ({{BAIRRO1}} etc — via subs)
#   1: pop_total       2: pop_target (<19)
#   3: tt_matriculas   4: penetracao
#   5: (separador)
#   6: tt_privadas     7: tt_publicas
#   8: pct_privadas    9: pct_publicas
#   10: (separador)
#   11-17: classes sociais (A++ A+ B1 B2 C1 C2 D/E) — apenas linha 0 tem headers
#           mas os valores ficam nas colunas 11-17 de cada linha de dados
# ─────────────────────────────────────────────────────
def atualizar_tabela_slide2(xml_bytes, dados):
    tabela = dados.get("bairros", {}).get("tabela", [])
    if not tabela:
        return xml_bytes

    tree = etree.fromstring(xml_bytes)
    tables = tree.findall(f".//{{{NS_A}}}tbl")
    if not tables:
        return xml_bytes

    tbl = tables[0]
    rows = tbl.findall(f"{{{NS_A}}}tr")

    # Mapa de índice de coluna → chave no dict da microárea
    # col 0: nome (via subs, não tocar)
    COL_MAP = {
        1:  ("pop_total",    fmt_num),
        2:  ("pop_target",   fmt_num),
        3:  ("tt_matriculas",fmt_num),
        4:  ("penetracao",   str),
        6:  ("tt_privadas",  fmt_num),
        7:  ("tt_publicas",  fmt_num),
        8:  ("pct_privadas", str),
        9:  ("pct_publicas", str),
    }
    # Classes sociais: cols 11-17 → A++ A+ B1 B2 C1 C2 D/E
    # Chaves batem com data_fetcher: pct_classe_App, pct_classe_Ap, etc.
    CLASS_COLS = {
        11: "pct_classe_App",
        12: "pct_classe_Ap",
        13: "pct_classe_B1",
        14: "pct_classe_B2",
        15: "pct_classe_C1",
        16: "pct_classe_C2",
        17: "pct_classe_D_E",
    }

    def set_cell(cell, value):
        t_els = list(cell.iter(f"{{{NS_A}}}t"))
        if t_els:
            t_els[0].text = str(value)
            for t in t_els[1:]:
                t.text = ""

    # Linhas 1-6 = dados das microáreas (linha 0 = cabeçalho)
    for r_idx, row in enumerate(rows):
        if r_idx == 0:
            continue  # cabeçalho — coberto por subs
        ma_idx = r_idx - 1
        if ma_idx >= len(tabela):
            break
        ma = tabela[ma_idx]
        cells = row.findall(f"{{{NS_A}}}tc")

        for c_idx, cell in enumerate(cells):
            if c_idx in COL_MAP:
                chave, fmt = COL_MAP[c_idx]
                valor = ma.get(chave, 0)
                set_cell(cell, fmt(valor))
            elif c_idx in CLASS_COLS:
                chave_pct = CLASS_COLS[c_idx]
                valor = ma.get(chave_pct, "—")
                set_cell(cell, str(valor))

    return etree.tostring(tree, xml_declaration=True, encoding="UTF-8", standalone=True)


# ─────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────
def gerar_relatorio(template, output, cidade, dados):
    subs = montar_subs(cidade, dados)

    with zipfile.ZipFile(template, "r") as zin:
        infos = {info.filename: info for info in zin.infolist()}
        files = {f: zin.read(f) for f in zin.namelist()}

    marketshare = dados.get("marketshare", {})
    evolucao    = dados.get("evolucao", {})
    top10       = dados.get("top10", {})
    bairros     = dados.get("bairros", {})

    # ── 1. Atualiza dados numéricos dos gráficos ──────────
    for path, (fase, tipo) in CHART_MAP.items():
        if path not in files:
            continue

        if tipo == "barras":
            ms = marketshare.get(fase)
            if ms:
                files[path] = atualizar_barras(
                    files[path],
                    ms.get("cats", []),
                    ms.get("vals", []),
                    ms.get("pct", None),
                )

        elif tipo == "linhas" and fase == "evolucao":
            if evolucao:
                files[path] = atualizar_linhas(
                    files[path],
                    evolucao.get("anos", []),
                    evolucao.get("series", []),
                    series_labels=["Infantil", "Fund. I", "Fund. II", "M\u00e9dio"],
                )

        elif tipo == "linhas" and fase == "top10":
            src = top10 if top10 else evolucao
            if src:
                escolas = src.get("escolas", [f"Escola {i}" for i in range(1, 11)])
                files[path] = atualizar_linhas(
                    files[path],
                    src.get("anos", []),
                    src.get("series", []),
                    series_labels=escolas,
                )

        elif tipo == "dist":
            cats = bairros.get("cats", [])
            vals = bairros.get("vals", [])
            if cats and vals:
                files[path] = atualizar_barras(files[path], cats, vals)

    # ── 2. Substitui placeholders de texto nos slides ─────
    for f in list(files.keys()):
        if f.startswith("ppt/slides/slide") and f.endswith(".xml"):
            files[f] = substituir_texto(files[f], subs)

    # ── 3. Atualiza tabelas com valores fixos ─────────────
    slide1 = "ppt/slides/slide1.xml"
    if slide1 in files:
        files[slide1] = atualizar_tabela_microareas(files[slide1], dados)

    slide2 = "ppt/slides/slide2.xml"
    if slide2 in files:
        files[slide2] = atualizar_tabela_slide2(files[slide2], dados)

    slide3 = "ppt/slides/slide3.xml"
    if slide3 in files:
        files[slide3] = atualizar_tabela_cagr(files[slide3], dados)

    # ── 4. Reempacota preservando metadados originais ─────
    with zipfile.ZipFile(output, "w") as zout:
        for fname, content in files.items():
            info = infos.get(fname)
            new_info = zipfile.ZipInfo(fname)
            new_info.compress_type = info.compress_type if info else zipfile.ZIP_DEFLATED
            new_info.date_time = info.date_time if info else (2024, 1, 1, 0, 0, 0)
            zout.writestr(new_info, content)


def executar_pipeline(template, output, cidade, dados):
    return gerar_relatorio(template, output, cidade, dados)

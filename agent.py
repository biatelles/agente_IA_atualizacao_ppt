"""
agent.py — agente inteligente com LLM

O agente usa GPT para interpretar o comando em linguagem natural e extrair
todos os parâmetros necessários para o pipeline:
- cidade
- ano_pontual (análise de market share)
- anos_evolucao (lista de anos para curvas de crescimento)
- anos_cagr (dict com os períodos solicitados)
- bairros (lista explícita ou "automatico")

O resultado alimenta data_fetcher e pipeline diretamente.
"""

import json
import re
from openai import OpenAI
from config import OPENAI_MODEL, OPENAI_MAX_TOKENS
from executor import executar_a_partir_de_params_completo

client = OpenAI()


# ─────────────────────────────────────────────────────────────
# PROMPT DO SISTEMA
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
Você é um assistente especializado em análise de mercado educacional.
Seu papel é interpretar comandos em linguagem natural e extrair os parâmetros
necessários para gerar um relatório de expansão de rede escolar.

Retorne APENAS um JSON válido, sem texto adicional, sem markdown, sem explicações.

Estrutura obrigatória do JSON:
{
  "cidade": "nome da cidade",
  "ano_pontual": 2025,
  "anos_evolucao": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
  "anos_cagr": {
    "longo_prazo": [2010, 2025],
    "periodo_1":   [2020, 2021],
    "periodo_2":   [2021, 2023],
    "periodo_3":   [2023, 2024],
    "periodo_4":   [2024, 2025]
  },
  "bairros": "automatico"
}

Regras:
- "bairros" pode ser "automatico" ou uma lista de strings: ["Centro", "Trindade"]
- "anos_cagr" deve ter tantas chaves quantos períodos forem mencionados
- Os períodos de CAGR são pares [ano_inicio, ano_fim]
- Se o usuário não mencionar algo explicitamente, use valores padrão razoáveis
- ano_pontual é o ano para análise de market share (snapshot)
- anos_evolucao é a sequência completa de anos para as curvas de crescimento
"""


# ─────────────────────────────────────────────────────────────
# EXTRAÇÃO DE PARÂMETROS VIA LLM
# ─────────────────────────────────────────────────────────────

def extrair_params(comando):
    """
    Envia o comando para o LLM e retorna os parâmetros como dicionário Python.
    """
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        max_tokens=OPENAI_MAX_TOKENS,
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": comando},
        ]
    )

    raw = response.choices[0].message.content.strip()

    # Remove blocos markdown caso o modelo os inclua mesmo sendo instruído a não
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$",     "", raw)

    try:
        params = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM retornou JSON inválido:\n{raw}\n\nErro: {e}")

    return params


# ─────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────────────────────

def processar_comando(comando, historico=None):

    if historico is None:
        historico = []

    print(f"📨 Comando recebido: {comando}")

    # ── 1. LLM extrai os parâmetros ───────────────────────────
    print("🧠 Interpretando comando com LLM...")
    try:
        params = extrair_params(comando)
        print(f"📋 Parâmetros extraídos:")
        print(json.dumps(params, ensure_ascii=False, indent=2))
    except Exception as e:
        msg = f"Erro ao interpretar comando: {e}"
        print(f"❌ {msg}")
        historico.append({"usuario": comando, "agente": msg})
        return {"resposta": msg, "ferramentas_usadas": [], "params_pipeline": None}, historico

    # ── 2. Executa o pipeline ─────────────────────────────────
    print("⚙️  Executando pipeline...")
    try:
        output = executar_a_partir_de_params_completo(params)
        resposta_texto  = f"Relatório gerado com sucesso para {params['cidade']}."
        ferramentas     = ["extrair_params", "carregar_dados", "gerar_relatorio"]
        params_pipeline = {**params, "output": output}
        print(f"✅ {resposta_texto}")
        print(f"📄 Arquivo: {output}")
    except Exception as e:
        resposta_texto  = f"Erro ao gerar relatório: {e}"
        ferramentas     = ["extrair_params"]
        params_pipeline = None
        print(f"❌ {resposta_texto}")

    # ── 3. Monta resposta ─────────────────────────────────────
    resultado = {
        "resposta":           resposta_texto,
        "ferramentas_usadas": ferramentas,
        "params_pipeline":    params_pipeline,
    }

    historico.append({"usuario": comando, "agente": resposta_texto})

    return resultado, historico

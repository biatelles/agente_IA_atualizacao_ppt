"""
executor.py — versão com suporte a parâmetros completos do agente

Expõe duas funções:
- executar_a_partir_de_params_completo(params): recebe o dict do agente LLM
- executar_a_partir_de_params(cidade):          compat. com uso simples anterior
"""

import os
from datetime import datetime

from config import TEMPLATE_PATH, OUTPUT_DIR
from data_fetcher import buscar_tudo
from pipeline import gerar_relatorio


def executar_a_partir_de_params_completo(params):
    """
    Recebe o dicionário completo extraído pelo agente LLM:
    {
        "cidade":        "Florianópolis",
        "ano_pontual":   2025,
        "anos_evolucao": [2016, ..., 2025],
        "anos_cagr":     {"longo_prazo": [2010,2025], ...},
        "bairros":       "automatico"
    }
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cidade        = params["cidade"]
    ano_pontual   = params.get("ano_pontual",   2025)
    anos_evolucao = params.get("anos_evolucao", list(range(2016, 2026)))
    anos_cagr     = params.get("anos_cagr",     {})
    bairros       = params.get("bairros",       "automatico")

    # Passa todos os parâmetros para o data_fetcher
    dados = buscar_tudo(
        cidade,
        ano_pontual=ano_pontual,
        anos_evolucao=anos_evolucao,
        anos_cagr=anos_cagr,
        bairros=bairros,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output    = os.path.join(OUTPUT_DIR, f"Relatorio_{cidade}_{timestamp}.pptx")

    gerar_relatorio(TEMPLATE_PATH, output, cidade, dados)

    return output


def executar_a_partir_de_params(cidade):
    """Compat. com uso simples — só passa a cidade."""
    return executar_a_partir_de_params_completo({"cidade": cidade})

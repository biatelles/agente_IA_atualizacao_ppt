"""
helpers.py — funções auxiliares para o notebook

Uso:
    from helpers import rodar, recarregar

    historico = rodar("Gere o relatório para Florianópolis...")
"""

import importlib
import sys


def recarregar():
    """Recarrega todos os módulos do projeto. Rode sempre após salvar arquivos."""
    modulos = ["config", "data_fetcher", "pipeline", "executor", "agent"]
    for nome in modulos:
        if nome in sys.modules:
            importlib.reload(sys.modules[nome])
    # Reimporta processar_comando do agent recarregado
    global _processar_comando
    from agent import processar_comando as _processar_comando
    print("✅ Módulos recarregados:", modulos)


# Carrega na primeira importação
from agent import processar_comando as _processar_comando


def rodar(comando, historico=None):
    """
    Executa um comando no agente e retorna o histórico atualizado.

    Parâmetros:
        comando   (str): instrução em linguagem natural
        historico (list): histórico de conversas anteriores (opcional)

    Retorna:
        historico (list): histórico atualizado
    """
    if historico is None:
        historico = []

    print(f"📨 Comando: {comando}")
    print("-" * 60)

    resultado, historico = _processar_comando(comando, historico)

    print(f"\n🤖 Agente: {resultado.get('resposta', '—')}")
    print(f"🔧 Ferramentas: {resultado.get('ferramentas_usadas', [])}")

    params = resultado.get("params_pipeline")
    if params and params.get("output"):
        print(f"📄 Arquivo: {params['output']}")

    return historico

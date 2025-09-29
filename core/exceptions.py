"""
Módulo de exceções personalizadas para o Editor Sapiens.
Fornece uma hierarquia clara de exceções para melhor tratamento de erros.
"""

class SapiensError(Exception):
    """Classe base para todas as exceções do Editor Sapiens."""
    pass

class ResourceError(SapiensError):
    """Erros relacionados a recursos (arquivos, memória, GPU)."""
    pass

class ProcessingError(SapiensError):
    """Erros durante o processamento de dados."""
    pass

class ValidationError(SapiensError):
    """Erros de validação de dados ou configurações."""
    pass

class StateError(SapiensError):
    """Erros relacionados a estados inválidos."""
    pass

class InterruptedError(SapiensError):
    """Erro quando uma operação é interrompida pelo usuário."""
    pass
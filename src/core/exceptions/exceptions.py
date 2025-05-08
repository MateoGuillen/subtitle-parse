""" Custom Exceptions for the pipeline """
# src/core/exceptions/exceptions.py

class PipelineError(Exception):
    """Base class for pipeline errors"""
    def __init__(self, message: str):
        super().__init__(message)


class ExtractionError(PipelineError):
    """Error while extracting data"""
    def __init__(self, source: str, message: str):
        full_message = f"[EXTRACTION ERROR] Fuente: {source} - {message}"
        super().__init__(full_message)


class TransformationError(PipelineError):
    """Error while transforming data"""
    def __init__(self, etapa: str, message: str):
        full_message = f"[TRANSFORMATION ERROR] Etapa: {etapa} - {message}"
        super().__init__(full_message)


class LoadError(PipelineError):
    """Error while loading data"""
    def __init__(self, destino: str, message: str):
        full_message = f"[LOAD ERROR] Destino: {destino} - {message}"
        super().__init__(full_message)


class LoggingSetupError(PipelineError):
    """Error while setting up logging"""
    def __init__(self, message: str):
        full_message = f"[LOGGING ERROR] {message}"
        super().__init__(full_message)

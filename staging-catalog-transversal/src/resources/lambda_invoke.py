import json
import boto3


class LambdaClient:
    """Cliente para invocar funciones AWS Lambda."""
    def __init__(self, function_name: str, region: str):
        self.function_name = function_name
        self.client = boto3.client("lambda", region_name=region)

    def invoke(self, payload: dict, async_: bool = False):
        """Invoca la función Lambda con el payload dado.
        Args:
            payload (dict): Datos a enviar a la función Lambda.
            async_ (bool): Si es True, la invocación es asíncrona.
        Returns:
            El resultado de la invocación si es síncrona, o None si es asíncrona.
        Raises:
            RuntimeError: Si la invocación falla o si Lambda devuelve un error.
        """
        resp = self.client.invoke(
            FunctionName=self.function_name,
            InvocationType="Event" if async_ else "RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        # Invocación asíncrona: Lambda no devolverá un payload de resultado de ejecución
        if async_:
            code = resp.get("StatusCode", 0)
            if code not in (202, 204):
                raise RuntimeError(f"Async invoke failed (StatusCode={code})")
            return None

        # Invocación síncrona: leer el payload devuelto
        raw = resp["Payload"].read()
        text = raw.decode("utf-8", errors="replace")

        # Si Lambda devolvió un error, AWS establece FunctionError y el payload contiene detalles del error
        if resp.get("FunctionError"):
            raise RuntimeError(f"Lambda error: {text}")

        # Si el payload no es JSON, devolver texto plano
        try:
            return json.loads(text) if text else None
        except json.JSONDecodeError:
            return text

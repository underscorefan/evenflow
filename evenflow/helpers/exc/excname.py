
def get_name(e: Exception) -> str:
    return type(e).__name__

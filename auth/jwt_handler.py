from datetime import datetime, timedelta, timezone
import time
from typing import Dict, Any, Optional
from uuid import UUID as PyUUID
from config import settings

import jwt
from jwt import PyJWTError
import logging

logger = logging.getLogger(__name__)

# NOTE: In a real application, SECRET_KEY should be loaded securely from an environment variable.
JWT_SECRET = settings.jwt_secret
JWT_ALGORITHM = "HS256"

# Define a type hint for the decoded token payload
DecodedPayload = Dict[str, Any]

class JWTHandler:
    """
    Handles JWT encoding, decoding, and validation.
    """

    @staticmethod
    def sign_jwt(user_id: PyUUID) -> Dict[str, str]:
        """
        Creates a JWT token payload containing the user ID and expiration time.
        """
        payload = {
            "user_id": str(user_id),
            "exp": datetime.now(timezone.utc) + timedelta(days=1),
            "iat": datetime.now(timezone.utc)
        }
        
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        logger.info(f"JWT created for user: {user_id}")
        return {"token": token}

    @staticmethod
    def decode_jwt(token: str) -> Optional[DecodedPayload]:
        """
        Decodes and validates the JWT token.

        Returns:
            The decoded payload (dict) or None if validation fails.
        """
        try:
            # 1. Decode the token using the secret and algorithm
            decoded_token: DecodedPayload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            
            # 2. Check for expiration
            if decoded_token.get("exp", 0) >= time.time():
                # Ensure user_id is present and is a string for type consistency
                if decoded_token.get("user_id") and isinstance(decoded_token["user_id"], str):
                    return decoded_token
                else:
                    logger.warning("JWT validation failed: Missing or invalid user_id in payload.")
                    return None
            
            logger.warning("JWT validation failed: Token expired.")
            return None
        except PyJWTError as e:
            logger.error(f"JWT decoding failed: {e}")
            return None
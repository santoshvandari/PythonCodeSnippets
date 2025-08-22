from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework import permissions

from django.contrib.auth import authenticate
from utils import get_tokens_for_user
from rest_framework import serializers
import logging


# Configure logging
logger = logging.getLogger(__name__)

class LoginSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(write_only=True)





# User Login View
class LoginView(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        """
        Logs in a user and returns a token.
        """
        try:
            serializer = LoginSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
            email = serializer.validated_data.get('email')
            password = serializer.validated_data.get('password')
            
            user = authenticate(request, username=email, password=password)

            if user is not None:
                if user.is_active:
                    token = get_tokens_for_user(user)
                    reponse = {
                        "email": user.email,
                        "full_name": user.full_name,
                        "role": user.role,
                    }
                    reponse.update(token)
                    return Response(reponse, status=status.HTTP_200_OK)

                return Response({"error": "User is inactive"}, status=status.HTTP_403_FORBIDDEN)
            return Response({"error": "Invalid credentials"}, status=status.HTTP_401_UNAUTHORIZED)
        except Exception as e:
            logger.error(f"Login error: {e}")
            return Response({"error": "An error occurred during login"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
      
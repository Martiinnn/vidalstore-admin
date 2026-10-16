import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException
} from '@nestjs/common';
import { CognitoJwtVerifier } from 'aws-jwt-verify';

@Injectable()
export class CognitoAuthGuard implements CanActivate {
  private verifier = CognitoJwtVerifier.create({
    userPoolId: process.env.COGNITO_USER_POOL_ID || '',
    tokenUse: 'access',
    clientId: process.env.COGNITO_CLIENT_ID || null
  });

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest();
    const authHeader = request.headers['authorization'];

    if (!authHeader) {
      throw new UnauthorizedException('Cabecera Authorization no proporcionada');
    }

    const [scheme, token] = authHeader.split(' ');
    if (scheme !== 'Bearer' || !token) {
      throw new UnauthorizedException('Formato de autorizacion invalido');
    }

    try {
      const payload = await this.verifier.verify(token);

      request.user = {
        sub: payload.sub,
        username: payload.username,
        clientId: payload.client_id,
        roles: (payload['cognito:groups'] as string[]) || []
      };

      return true;
    } catch (error: any) {
      throw new UnauthorizedException(`Token invalido o no autorizado: ${error.message}`);
    }
  }
}

import {
  Controller,
  Get,
  Post,
  Delete,
  Body,
  Param,
  UseGuards,
  HttpCode,
  HttpStatus
} from '@nestjs/common';
import { RabbitAdminService } from './rabbit-admin.service';
import { CrearColaDto } from './dto/crear-cola.dto';
import { CrearExchangeDto } from './dto/crear-exchange.dto';
import { CrearBindingDto } from './dto/crear-binding.dto';
import { EliminarBindingDto } from './dto/eliminar-binding.dto';
import { CognitoAuthGuard } from '../auth/cognito-auth.guard';
import { RolesGuard } from '../auth/roles.guard';
import { Roles } from '../auth/roles.decorator';

@Controller('v1/admin')
export class AdminController {
  constructor(private readonly adminService: RabbitAdminService) {}

  @Get('salud')
  obtenerSalud() {
    return {
      estado: 'activo',
      servicio: 'vidalstore-admin',
      timestamp: new Date().toISOString()
    };
  }

  @Get('colas')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  listarColas() {
    return this.adminService.listarColas();
  }

  @Post('colas')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.CREATED)
  crearCola(@Body() dto: CrearColaDto) {
    return this.adminService.crearCola(dto.nombre, dto.durable, dto.argumentos);
  }

  @Delete('colas/:nombre')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.OK)
  eliminarCola(@Param('nombre') nombre: string) {
    return this.adminService.eliminarCola(nombre);
  }

  @Get('exchanges')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  listarExchanges() {
    return this.adminService.listarExchanges();
  }

  @Post('exchanges')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.CREATED)
  crearExchange(@Body() dto: CrearExchangeDto) {
    return this.adminService.crearExchange(dto.nombre, dto.tipo, dto.durable);
  }

  @Delete('exchanges/:nombre')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.OK)
  eliminarExchange(@Param('nombre') nombre: string) {
    return this.adminService.eliminarExchange(nombre);
  }

  @Post('bindings')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.CREATED)
  crearBinding(@Body() dto: CrearBindingDto) {
    return this.adminService.crearBinding(dto.exchange, dto.cola, dto.routingKey);
  }

  @Delete('bindings')
  @UseGuards(CognitoAuthGuard, RolesGuard)
  @Roles('administradores')
  @HttpCode(HttpStatus.OK)
  eliminarBinding(@Body() dto: EliminarBindingDto) {
    return this.adminService.eliminarBinding(dto.exchange, dto.cola, dto.routingKey);
  }
}

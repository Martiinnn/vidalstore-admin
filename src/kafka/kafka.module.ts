import { Module } from '@nestjs/common';
import { KafkaAdminController } from './kafka-admin.controller';
import { KafkaAdminService } from './kafka-admin.service';

@Module({
  controllers: [KafkaAdminController],
  providers: [KafkaAdminService],
  exports: [KafkaAdminService],
})
export class KafkaModule {}

import { Module } from '@nestjs/common';
import { AdminModule } from './admin/admin.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [AdminModule, KafkaModule]
})
export class AppModule {}

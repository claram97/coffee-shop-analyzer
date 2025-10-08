"""
Protobuf adapter for Filter Router and Workers.
Converts between protobuf messages and legacy DataBatch/EOFMessage objects.
"""

import logging
from typing import Any, Union
from types import SimpleNamespace

from protos import envelope_pb2, databatch_pb2, eof_message_pb2, table_data_pb2
from protocol.databatch import DataBatch
from protocol.messages import EOFMessage
from protocol.constants import Opcodes

logger = logging.getLogger(__name__)

# Mapeo entre TableName protobuf (enum 0-4) y Opcodes legacy (int 4-8)
TABLENAME_TO_OPCODE = {
    table_data_pb2.TableName.MENU_ITEMS: Opcodes.NEW_MENU_ITEMS,
    table_data_pb2.TableName.STORES: Opcodes.NEW_STORES,
    table_data_pb2.TableName.TRANSACTION_ITEMS: Opcodes.NEW_TRANSACTION_ITEMS,
    table_data_pb2.TableName.TRANSACTIONS: Opcodes.NEW_TRANSACTION,
    table_data_pb2.TableName.USERS: Opcodes.NEW_USERS,
}

# Mapeo inverso: Opcodes legacy → TableName protobuf
OPCODE_TO_TABLENAME = {v: k for k, v in TABLENAME_TO_OPCODE.items()}



def deserialize_envelope_from_rabbitmq(body: bytes) -> Union[DataBatch, EOFMessage, None]:
    """
    Deserializa un Envelope protobuf desde RabbitMQ y lo convierte al formato legacy.
    
    Args:
        body: Bytes del mensaje protobuf Envelope
        
    Returns:
        DataBatch o EOFMessage en formato legacy, o None si hay error
    """
    try:
        envelope = envelope_pb2.Envelope()
        envelope.ParseFromString(body)
        
        if envelope.type == envelope_pb2.Envelope.DATA_BATCH:
            if envelope.HasField('data_batch'):
                return protobuf_databatch_to_legacy(envelope.data_batch)
            else:
                logger.error("Envelope DATA_BATCH sin campo data_batch")
                return None
                
        elif envelope.type == envelope_pb2.Envelope.EOF:
            if envelope.HasField('eof'):
                return protobuf_eof_to_legacy(envelope.eof)
            else:
                logger.error("Envelope EOF sin campo eof")
                return None
        else:
            logger.warning(f"Envelope type no soportado: {envelope.type}")
            return None
            
    except Exception as e:
        logger.exception(f"Error deserializando Envelope: {e}")
        return None


def protobuf_databatch_to_legacy(pb_batch: databatch_pb2.DataBatch) -> DataBatch:
    """
    Convierte un DataBatch protobuf al formato legacy DataBatch.
    """
    legacy_batch = DataBatch()
    
    # Mapear query_ids (enum Query → int 1-4)
    legacy_batch.query_ids = [int(q) + 1 for q in pb_batch.query_ids]
    
    # Mapear filter_steps → reserved_u16 (bitmask)
    legacy_batch.reserved_u16 = pb_batch.filter_steps
    
    # Mapear sharding info
    if pb_batch.total_shards > 0:
        legacy_batch.shards_info = [(pb_batch.total_shards, pb_batch.shard_num)]
    else:
        legacy_batch.shards_info = []
    
    # Copiar fan-out info
    legacy_batch.total_copies = pb_batch.total_copies
    legacy_batch.copy_num = pb_batch.copy_num
    
    # Crear batch_msg desde payload (TableData)
    if pb_batch.HasField('payload'):
        legacy_batch.batch_msg = protobuf_tabledata_to_legacy_batchmsg(pb_batch.payload)
        legacy_batch.batch_number = pb_batch.payload.batch_number
    
    return legacy_batch


class LegacyBatchMessage:
    """Wrapper que simula el comportamiento de batch_msg legacy con método to_bytes()."""
    
    def __init__(self, table_data: table_data_pb2.TableData):
        self._table_data = table_data
        # Mapear TableName protobuf → Opcode legacy
        self.opcode = TABLENAME_TO_OPCODE.get(table_data.name, int(table_data.name))
        self.batch_number = table_data.batch_number
        self.batch_status = int(table_data.status)
        self.amount = len(table_data.rows)
        self.rows = []
        
        # Convertir rows de protobuf a objetos con atributos
        if table_data.HasField('schema'):
            columns = list(table_data.schema.columns)
            
            for pb_row in table_data.rows:
                row_obj = SimpleNamespace()
                for i, col_name in enumerate(columns):
                    if i < len(pb_row.values):
                        setattr(row_obj, col_name, pb_row.values[i])
                self.rows.append(row_obj)
    
    def to_bytes(self) -> bytes:
        """
        Convierte de vuelta a protobuf bytes.
        Este método es llamado por el código legacy del Filter.
        """
        # Actualizar el TableData con las rows modificadas (si cambiaron)
        updated_table_data = table_data_pb2.TableData()
        updated_table_data.CopyFrom(self._table_data)
        
        # Si rows fueron modificadas, actualizar el protobuf
        if self.rows:
            # Extraer columnas
            if self.rows and hasattr(self.rows[0], '__dict__'):
                columns = [k for k in self.rows[0].__dict__.keys() if not k.startswith('_')]
                
                # Limpiar y reconstruir rows
                del updated_table_data.rows[:]
                updated_table_data.schema.columns[:] = columns
                
                for row_obj in self.rows:
                    pb_row = table_data_pb2.Row()
                    for col in columns:
                        value = getattr(row_obj, col, '')
                        pb_row.values.append(str(value))
                    updated_table_data.rows.append(pb_row)
        
        return updated_table_data.SerializeToString()


def protobuf_tabledata_to_legacy_batchmsg(table_data: table_data_pb2.TableData) -> LegacyBatchMessage:
    """
    Convierte TableData protobuf a un objeto batch_msg legacy.
    """
    return LegacyBatchMessage(table_data)


def protobuf_eof_to_legacy(pb_eof: eof_message_pb2.EOFMessage) -> EOFMessage:
    """
    Convierte EOFMessage protobuf al formato legacy EOFMessage.
    """
    legacy_eof = EOFMessage()
    
    # Mapear TableName enum → string
    table_name_str = table_data_pb2.TableName.Name(pb_eof.table).lower()
    legacy_eof.table_type = table_name_str
    
    return legacy_eof


def legacy_databatch_to_protobuf(legacy_batch: DataBatch) -> databatch_pb2.DataBatch:
    """
    Convierte un DataBatch legacy de vuelta a protobuf para enviar a RabbitMQ.
    """
    pb_batch = databatch_pb2.DataBatch()
    
    # Mapear query_ids (int 1-4 → enum Query 0-3)
    # Los enums en protobuf Python son valores enteros directamente
    pb_batch.query_ids.extend([q - 1 for q in legacy_batch.query_ids])
    
    # Mapear filter_steps
    pb_batch.filter_steps = getattr(legacy_batch, 'reserved_u16', 0)
    
    # Mapear sharding info
    shards_info = getattr(legacy_batch, 'shards_info', [])
    if shards_info:
        total_shards, shard_num = shards_info[-1]  # Tomar el último shard
        pb_batch.total_shards = total_shards
        pb_batch.shard_num = shard_num
    
    # Mapear fan-out info (total_copies, copy_num)
    pb_batch.total_copies = getattr(legacy_batch, 'total_copies', 1)
    pb_batch.copy_num = getattr(legacy_batch, 'copy_num', 0)
    
    # Convertir batch_msg → TableData payload
    batch_msg = getattr(legacy_batch, 'batch_msg', None)
    if batch_msg:
        # Si batch_msg es nuestro LegacyBatchMessage, obtener TableData actualizado
        if isinstance(batch_msg, LegacyBatchMessage):
            # Reconstruir TableData con rows posiblemente modificadas
            updated_table_data = table_data_pb2.TableData()
            # Mapear Opcode legacy → TableName protobuf
            updated_table_data.name = OPCODE_TO_TABLENAME.get(batch_msg.opcode, batch_msg.opcode)
            updated_table_data.batch_number = batch_msg.batch_number
            updated_table_data.status = batch_msg.batch_status
            
            # Reconstruir rows
            if batch_msg.rows:
                sample_row = batch_msg.rows[0]
                if hasattr(sample_row, '__dict__'):
                    columns = [k for k in sample_row.__dict__.keys() if not k.startswith('_')]
                    updated_table_data.schema.columns[:] = columns
                    
                    for row_obj in batch_msg.rows:
                        pb_row = table_data_pb2.Row()
                        for col in columns:
                            value = getattr(row_obj, col, '')
                            pb_row.values.append(str(value))
                        updated_table_data.rows.append(pb_row)
            
            pb_batch.payload.CopyFrom(updated_table_data)
        else:
            # Fallback: convertir desde objeto legacy genérico
            pb_batch.payload.CopyFrom(legacy_batchmsg_to_protobuf_tabledata(batch_msg))
    
    return pb_batch


def legacy_batchmsg_to_protobuf_tabledata(batch_msg: Any) -> table_data_pb2.TableData:
    """
    Convierte batch_msg legacy → TableData protobuf.
    """
    table_data = table_data_pb2.TableData()
    
    # Mapear opcode legacy → TableName protobuf
    opcode = getattr(batch_msg, 'opcode', 0)
    table_data.name = OPCODE_TO_TABLENAME.get(opcode, opcode)
    table_data.batch_number = getattr(batch_msg, 'batch_number', 0)
    table_data.status = getattr(batch_msg, 'batch_status', table_data_pb2.BatchStatus.CONTINUE)
    
    # Convertir rows (objetos con atributos → Row protobuf)
    rows = getattr(batch_msg, 'rows', [])
    if rows:
        # Extraer columnas del primer row
        sample_row = rows[0]
        if hasattr(sample_row, '__dict__'):
            columns = [k for k in sample_row.__dict__.keys() if not k.startswith('_')]
        else:
            columns = []
        
        # Crear schema
        table_data.schema.columns.extend(columns)
        
        # Convertir cada row
        for row_obj in rows:
            pb_row = table_data_pb2.Row()
            for col in columns:
                value = getattr(row_obj, col, '')
                pb_row.values.append(str(value))
            table_data.rows.append(pb_row)
    
    return table_data


def legacy_eof_to_protobuf(legacy_eof: EOFMessage) -> eof_message_pb2.EOFMessage:
    """
    Convierte EOFMessage legacy → protobuf.
    """
    pb_eof = eof_message_pb2.EOFMessage()
    
    # Mapear table_type string → TableName enum
    table_name_upper = legacy_eof.table_type.upper()
    pb_eof.table = table_data_pb2.TableName.Value(table_name_upper)
    
    return pb_eof


def serialize_to_envelope_bytes(obj: Union[DataBatch, EOFMessage]) -> bytes:
    """
    Serializa un DataBatch o EOFMessage legacy → Envelope protobuf bytes.
    """
    envelope = envelope_pb2.Envelope()
    
    if isinstance(obj, DataBatch):
        envelope.type = envelope_pb2.Envelope.DATA_BATCH
        pb_batch = legacy_databatch_to_protobuf(obj)
        envelope.data_batch.CopyFrom(pb_batch)
        
    elif isinstance(obj, EOFMessage):
        envelope.type = envelope_pb2.Envelope.EOF
        pb_eof = legacy_eof_to_protobuf(obj)
        envelope.eof.CopyFrom(pb_eof)
    else:
        raise ValueError(f"Tipo no soportado: {type(obj)}")
    
    return envelope.SerializeToString()

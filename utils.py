import io
import avro.io
import avro.schema
import avro.datafile


def json_to_avro(event, schema):
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), schema)
    writer.append(event)
    writer.flush()
    buf.seek(0)
    data = buf.read()

    return data


def avro_to_json(event):
    message_buf = io.BytesIO(event)
    reader = avro.datafile.DataFileReader(message_buf, avro.io.DatumReader())
    data = [l for l in reader]
    reader.close()

    return data

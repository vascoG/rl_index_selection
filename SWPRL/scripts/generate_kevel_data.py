import pandas as pd
from sqlalchemy import create_engine
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata

if __name__ == '__main__':
    metadata = SingleTableMetadata()

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/kevel_sample')

    data = pd.read_sql('SELECT * FROM order_storage.orders_timeline', engine)
    metadata.detect_from_dataframe(data)
    metadata.update_column(column_name='id', sdtype='id', regex_format="1[0-4][0-9]{7}")
    metadata.update_column(column_name='order_id', sdtype='id', regex_format="[0-9]{9}")
    metadata.update_column(column_name='timestamp', sdtype='numerical')
    metadata.set_primary_key('id')

    model = GaussianCopulaSynthesizer(metadata)
    model.fit(data)

    new_data = model.sample(num_rows = 30660000, batch_size = 10000, output_file_path = "kevel_data2.csv")  # last month of data
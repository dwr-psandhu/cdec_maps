from cdec_maps import cdec
import pandas as pd
import tqdm


def download_row(row, reader):
    unit = row["Units"]
    station_id = row["ID"]
    sensor_desc = row["Sensor"]
    sensor_number = row["Sensor Number"]
    duration_code = cdec.get_duration_code(row["Duration"])
    start = row["Start Date"]
    end = row["End Date"]
    df = reader.read_station_data(station_id, sensor_number, duration_code, start, end)
    return df, unit, sensor_desc


def download_all():
    reader = cdec.Reader()
    stations, sensor_list, stations_meta_info = reader.read_saved_stations_info()
    # Assuming stations_meta_info is your dataframe
    stations_meta_info[["Start Date", "End Date"]] = stations_meta_info[
        "Data Available"
    ].str.split(" to ", expand=True)
    # Replace 'present' with current date and convert 'End Date' to datetime
    stations_meta_info["End Date"] = stations_meta_info["End Date"].replace(
        "present", pd.to_datetime("today").strftime("%m/%d/%Y")
    )
    # Convert 'Start Date' to datetime
    stations_meta_info["Start Date"] = pd.to_datetime(
        stations_meta_info["Start Date"], errors="coerce"
    )
    stations_meta_info["End Date"] = pd.to_datetime(
        stations_meta_info["End Date"], errors="coerce"
    )

    for index in tqdm.tqdm(range(len(stations_meta_info))):
        row = stations_meta_info.iloc[index]
        try:
            download_row(row, reader)
        except Exception as e:
            print(f"Failed to download data for row at {index}: {e}")


if __name__ == "__main__":
    download_all()

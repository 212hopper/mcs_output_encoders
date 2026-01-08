import requests
import json
import urllib3
import os
import base64
import time
import pyodbc

# === CONFIG - READ FROM ENVIRONMENT VARIABLES ===
mcs_ip_primary = os.getenv('MCS_IP_PRIMARY')
mcs_ip_secondary = os.getenv('MCS_IP_SECONDARY')
access_port = os.getenv('MCS_PORT', '443')

if not mcs_ip_primary or not mcs_ip_secondary:
    raise Exception("MCS_IP_PRIMARY and MCS_IP_SECONDARY environment variables are required")

base_primary = f"https://{mcs_ip_primary}:{access_port}"
base_secondary = f"https://{mcs_ip_secondary}:{access_port}"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
token_file = os.path.join(SCRIPT_DIR, "tokens", "token_storage.json")
os.makedirs(os.path.dirname(token_file), exist_ok=True)

username = os.getenv('MCS_USERNAME')
password = os.getenv('MCS_PASSWORD')

if not username or not password:
    raise Exception("MCS_USERNAME and MCS_PASSWORD environment variables are required")

# === SQL SERVER CONFIG ===
SQL_SERVER = os.getenv('SQL_SERVER')
SQL_DATABASE = os.getenv('SQL_DATABASE')
SQL_USERNAME = os.getenv('SQL_USERNAME')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')

if not SQL_SERVER or not SQL_USERNAME or not SQL_PASSWORD:
    raise Exception("SQL_SERVER, SQL_USERNAME, and SQL_PASSWORD environment variables are required")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# === Pure-Python JWT exp extractor ===
def get_jwt_exp(token):
    try:
        payload = token.split('.')[1]
        payload += '=' * (-len(payload) % 4)
        decoded = base64.b64decode(payload)
        return json.loads(decoded).get("exp", 0)
    except:
        return 0


def load_token():
    if not os.path.exists(token_file):
        return None
    try:
        with open(token_file, "r") as f:
            data = json.load(f)
        exp = get_jwt_exp(data.get("access_token", ""))
        if exp and exp > time.time() + 60:
            data["exp"] = exp
            return data
    except Exception as e:
        print(f"Token cache invalid: {e}")
    return None


def save_token(access_token, refresh_token):
    exp = get_jwt_exp(access_token)
    cache = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "exp": exp
    }
    with open(token_file, "w") as f:
        json.dump(cache, f, indent=2)


def login(base_url):
    print(f"Logging in to {base_url}/api/5.1/auth/login ...")
    url = f"{base_url}/api/5.1/auth/login"
    r = requests.post(
        url,
        json={"username": username, "password": password},
        headers={"accept": "application/json", "content-type": "application/json"},
        verify=False,
        timeout=10
    )
    r.raise_for_status()
    data = r.json()["data"]
    save_token(data["access_token"], data["refresh_token"])
    return data["access_token"]


def refresh_token(base_url, refresh_token):
    print("Refreshing access token...")
    url = f"{base_url}/api/5.1/auth/token/refresh"
    r = requests.post(
        url,
        json={"refresh_token": refresh_token},
        headers={"accept": "application/json", "content-type": "application/json"},
        verify=False,
        timeout=10
    )
    if r.status_code >= 400:
        raise Exception("Refresh failed")
    data = r.json()["data"]
    save_token(data["access_token"], data["refresh_token"])
    return data["access_token"]


# === Global token cache ===
_cached_token = None
_refresh_token = None


def get_valid_token():
    global _cached_token, _refresh_token

    cache = load_token()
    if cache:
        _cached_token = cache["access_token"]
        _refresh_token = cache["refresh_token"]

    for base in [base_primary, base_secondary]:
        # Try refresh first if we have a refresh token
        if _refresh_token:
            try:
                _cached_token = refresh_token(base, _refresh_token)
                return _cached_token
            except:
                print("Refresh failed, will try login")

        # Fall back to full login
        try:
            _cached_token = login(base)
            cache = load_token()
            if cache:
                _refresh_token = cache["refresh_token"]
            return _cached_token
        except Exception as e:
            print(f"Login failed on {base}: {e}")

    raise Exception("Could not authenticate to either MCS")


def bearer_token():
    global _cached_token
    if not _cached_token:
        _cached_token = get_valid_token()
    return _cached_token


def send_api_get_call(full_path, base_url=base_primary):
    global _cached_token

    url = base_url + full_path
    headers = {
        "Authorization": f"Bearer {bearer_token()}",
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, headers=headers, verify=False, timeout=15)
        if r.status_code == 401:
            print("401 Unauthorized → forcing token refresh")
            _cached_token = None
            headers["Authorization"] = f"Bearer {bearer_token()}"
            r = requests.get(url, headers=headers, verify=False, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if "401" in str(e):
            print("Still 401 after retry → trying secondary")
            return send_api_get_call(full_path, base_secondary)
        raise


def send_api_put_call(full_path, json_payload, base_url=base_primary):
    global _cached_token

    url = base_url + full_path
    headers = {
        "Authorization": f"Bearer {bearer_token()}",
        "Accept": "application/json"
    }

    try:
        r = requests.put(url, headers=headers, verify=False, timeout=15, json=json_payload)
        if r.status_code == 401:
            print("401 Unauthorized → forcing token refresh")
            _cached_token = None
            headers["Authorization"] = f"Bearer {bearer_token()}"
            r = requests.put(url, headers=headers, verify=False, timeout=15, json=json_payload)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if "401" in str(e):
            print("Still 401 after retry → trying secondary")
            return send_api_put_call(full_path, json_payload, base_secondary)
        raise


# === API Functions ===
def get_all_outputs():
    try:
        return send_api_get_call("/api/5.1/outputs/config")
    except:
        print("Primary failed → trying secondary")
        return send_api_get_call("/api/5.1/outputs/config", base_secondary)


def get_one_output(uuid_of_source):
    try:
        return send_api_get_call(f"/api/5.1/outputs/config/{uuid_of_source}")
    except:
        print("Primary failed → trying secondary")
        return send_api_get_call(f"/api/5.1/outputs/config/{uuid_of_source}", base_secondary)


def get_all_devices():
    try:
        return send_api_get_call("/api/5.1/devices/config")
    except:
        print("Primary failed → trying secondary")
        return send_api_get_call("/api/5.1/devices/config", base_secondary)


def get_all_devices_status():
    try:
        return send_api_get_call("/api/5.1/devices/status")
    except:
        print("Primary failed → trying secondary")
        return send_api_get_call("/api/5.1/devices/status", base_secondary)


def put_one_output(uuid_of_source, payload_to_pass):
    try:
        return send_api_put_call(f"/api/5.1/outputs/config/{uuid_of_source}", payload_to_pass)
    except:
        print("Primary failed → trying secondary")
        return send_api_put_call(f"/api/5.1/outputs/config/{uuid_of_source}", payload_to_pass, base_secondary)


# === SQL Server Connection ===
def get_db_connection():
    return pyodbc.connect(
        f'Driver={{ODBC Driver 18 for SQL Server}};'
        f'Server={SQL_SERVER};'
        f'Database={SQL_DATABASE};'
        f'UID={SQL_USERNAME};'
        f'PWD={SQL_PASSWORD};'
        f'TrustServerCertificate=yes;'
    )


# === Database Functions ===
def get_devices_table():
    print("Fetching devices and updating database...")
    device_list = get_all_devices()
    devices_to_store = []
    
    for device in device_list.get("data", []):
        devices_to_store.append({
            "Device_UUID": device.get("uuid"),
            "Device_Label": device.get("label"),
            "Device_IP": device.get("access", {}).get("url")
        })
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for single_device in devices_to_store:
            cursor.execute("""
                MERGE INTO dbo.mcs_mcm_list AS target
                USING (SELECT ? AS uuid) AS source
                ON target.uuid = source.uuid
                WHEN MATCHED THEN
                    UPDATE SET label = ?, ip = ?
                WHEN NOT MATCHED THEN
                    INSERT (uuid, label, ip)
                    VALUES (?, ?, ?);
            """, (
                single_device['Device_UUID'],
                single_device['Device_Label'],
                single_device['Device_IP'],
                single_device['Device_UUID'],
                single_device['Device_Label'],
                single_device['Device_IP']
            ))
        
        conn.commit()
        conn.close()
        print(f"Updated {len(devices_to_store)} devices in database")
        return devices_to_store
    except Exception as e:
        print(f"Database error in get_devices_table: {e}")
        raise


def get_devices_status():
    print("Fetching device status and updating database...")
    device_list = get_all_devices_status()
    devices_to_store = []
    
    for device in device_list.get("data", []):
        devices_to_store.append({
            "Device_UUID": device.get("uuid"),
            "Status": device.get("status")
        })
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for single_device in devices_to_store:
            cursor.execute("""
                UPDATE dbo.mcs_mcm_list
                SET status = ?
                WHERE uuid = ?
            """, (
                single_device['Status'],
                single_device['Device_UUID']
            ))
        
        conn.commit()
        conn.close()
        print(f"Updated status for {len(devices_to_store)} devices in database")
        return devices_to_store
    except Exception as e:
        print(f"Database error in get_devices_status: {e}")
        raise


def get_output_per_mcm():
    print("Fetching outputs and updating database...")
    output_list = get_all_outputs()
    outputs_to_store = []
    
    for output in output_list.get("data", []):
        outputs_to_store.append({
            "Output_UUID": output.get("uuid"),
            "Output_Label": output.get("label"),
            "Output_Device_UUID": output.get("device")
        })
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for single_output in outputs_to_store:
            cursor.execute("""
                MERGE INTO dbo.mcs_output_list AS target
                USING (SELECT ? AS uuid) AS source
                ON target.uuid = source.uuid
                WHEN MATCHED THEN
                    UPDATE SET label = ?, device_uuid = ?
                WHEN NOT MATCHED THEN
                    INSERT (uuid, label, device_uuid)
                    VALUES (?, ?, ?);
            """, (
                single_output['Output_UUID'],
                single_output['Output_Label'],
                single_output['Output_Device_UUID'],
                single_output['Output_UUID'],
                single_output['Output_Label'],
                single_output['Output_Device_UUID']
            ))
        
        conn.commit()
        conn.close()
        print(f"Updated {len(outputs_to_store)} outputs in database")
        return outputs_to_store
    except Exception as e:
        print(f"Database error in get_output_per_mcm: {e}")
        raise


def get_mcm_outputs():
    print("Fetching MCM encoder outputs and updating database...")
    mcm_outputs_to_store = []
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT ip, status FROM dbo.mcs_mcm_list")
        devices = cursor.fetchall()
        conn.close()
        
        for device_ip, device_status in devices:
            if device_status == "Up":
                print(f"Getting encoder info for {device_ip}")
                try:
                    get_encoder_info = requests.get(
                        f"http://{device_ip}/api/2.0/outputs/config/.json",
                        auth=("Admin", "Admin"),
                        timeout=10,
                        verify=False
                    ).json()
                    
                    for encoder_item in get_encoder_info:
                        encoder_uuid = encoder_item["Encoder"]["uuid"]
                        encoder_id = encoder_item["Encoder"]["id"]
                        encoder_title = encoder_item["Encoder"]["title"]
                        encoder_enabled = encoder_item["Encoder"]["is_enabled"]
                        uuid = f"{device_ip}_{encoder_uuid}"
                        
                        mcm_outputs_to_store.append({
                            "Encoder_UUID": encoder_uuid,
                            "Encoder_ID": encoder_id,
                            "Encoder_Title": encoder_title,
                            "Encoder_Enabled": encoder_enabled,
                            "UUID": uuid
                        })
                except Exception as e:
                    print(f"Error fetching encoder info from {device_ip}: {e}")
        
        # Update database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for single_output in mcm_outputs_to_store:
            cursor.execute("""
                MERGE INTO dbo.mcs_mcm_outputs AS target
                USING (SELECT ? AS uuid) AS source
                ON target.uuid = source.uuid
                WHEN MATCHED THEN
                    UPDATE SET encoder_label = ?, encoder_uuid = ?, enabled = ?, encoder_id = ?
                WHEN NOT MATCHED THEN
                    INSERT (uuid, encoder_label, encoder_uuid, enabled, encoder_id)
                    VALUES (?, ?, ?, ?, ?);
            """, (
                single_output['UUID'],
                single_output['Encoder_Title'],
                single_output['Encoder_UUID'],
                single_output['Encoder_Enabled'],
                single_output['Encoder_ID'],
                single_output['UUID'],
                single_output['Encoder_Title'],
                single_output['Encoder_UUID'],
                single_output['Encoder_Enabled'],
                single_output['Encoder_ID']
            ))
        
        conn.commit()
        conn.close()
        print(f"Updated {len(mcm_outputs_to_store)} MCM outputs in database")
        
    except Exception as e:
        print(f"Database error in get_mcm_outputs: {e}")
        raise


# === Main Execution ===
if __name__ == "__main__":
    print("Starting MCS data collection...\n")
    print("Running every 10 minutes...\n")
    
    try:
        while True:
            print(f"Collection run at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            get_devices_table()
            get_devices_status()
            get_output_per_mcm()
            get_mcm_outputs()
            print(f"Completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("Waiting 10 minutes before next collection...\n")
            time.sleep(600)
    except KeyboardInterrupt:
        print("\nShutdown requested, exiting...")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
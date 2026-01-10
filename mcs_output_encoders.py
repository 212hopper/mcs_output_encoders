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


def get_db_connection():
    return pyodbc.connect(
        f'Driver={{ODBC Driver 18 for SQL Server}};'
        f'Server={SQL_SERVER};'
        f'Database={SQL_DATABASE};'
        f'UID={SQL_USERNAME};'
        f'PWD={SQL_PASSWORD};'
        f'TrustServerCertificate=yes;'
    )


def get_token_columns(base_url):
    """Return the column names for the given base_url"""
    if base_url == base_primary:
        return "token_access_primary", "token_refresh_primary", "token_exp_primary"
    else:
        return "token_access_secondary", "token_refresh_secondary", "token_exp_secondary"


def load_token(base_url):
    try:
        access_col, refresh_col, exp_col = get_token_columns(base_url)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT {access_col}, {refresh_col}, {exp_col} FROM dbo.mcs_admin WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        
        if not row or not row[0]:  # No row or token_access is null
            return None
        
        token_access, token_refresh, token_exp = row
        exp = int(token_exp) if token_exp else 0
        
        # Check if token is still valid (more than 60 seconds left)
        if exp and exp > time.time() + 60:
            return {
                "access_token": token_access,
                "refresh_token": token_refresh,
                "exp": exp
            }
    except Exception as e:
        print(f"Token load error: {e}")
    
    return None


def save_token(access_token, refresh_token, base_url):
    try:
        exp = get_jwt_exp(access_token)
        access_col, refresh_col, exp_col = get_token_columns(base_url)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE dbo.mcs_admin 
            SET {access_col} = ?, {refresh_col} = ?, {exp_col} = ?
            WHERE id = 1
        """, (access_token, refresh_token, int(exp)))
        conn.commit()
        conn.close()
        server = "PRIMARY" if base_url == base_primary else "SECONDARY"
        print(f"Token saved to database ({server})")
    except Exception as e:
        print(f"Token save error: {e}")
        raise


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
    save_token(data["access_token"], data["refresh_token"], base_url)
    return data["access_token"]


def refresh_token(base_url, refresh_token_str):
    print("Refreshing access token...")
    url = f"{base_url}/api/5.1/auth/token/refresh"
    r = requests.post(
        url,
        json={"refresh_token": refresh_token_str},
        headers={"accept": "application/json", "content-type": "application/json"},
        verify=False,
        timeout=10
    )
    if r.status_code >= 400:
        raise Exception("Refresh failed")
    data = r.json()["data"]
    save_token(data["access_token"], data["refresh_token"], base_url)
    return data["access_token"]


# === Global token cache ===
_cached_tokens = {}  # Store tokens per base_url
_refresh_tokens = {}  # Store refresh tokens per base_url


def get_valid_token(base_url):
    """Get valid token for the given base_url"""
    global _cached_tokens, _refresh_tokens

    cache = load_token(base_url)
    if cache:
        _cached_tokens[base_url] = cache["access_token"]
        _refresh_tokens[base_url] = cache["refresh_token"]

    # Try refresh first if we have a refresh token
    if base_url in _refresh_tokens:
        try:
            _cached_tokens[base_url] = refresh_token(base_url, _refresh_tokens[base_url])
            return _cached_tokens[base_url]
        except:
            print("Refresh failed, will try login")

    # Fall back to full login
    try:
        _cached_tokens[base_url] = login(base_url)
        cache = load_token(base_url)
        if cache:
            _refresh_tokens[base_url] = cache["refresh_token"]
        return _cached_tokens[base_url]
    except Exception as e:
        print(f"Login failed on {base_url}: {e}")
        raise Exception("Could not authenticate to MCS")


def bearer_token(base_url):
    """Get bearer token for the given base_url"""
    global _cached_tokens
    if base_url not in _cached_tokens or not _cached_tokens[base_url]:
        _cached_tokens[base_url] = get_valid_token(base_url)
    return _cached_tokens[base_url]


def send_api_get_call(full_path, base_url=base_primary):
    global _cached_tokens

    url = base_url + full_path
    headers = {
        "Authorization": f"Bearer {bearer_token(base_url)}",
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, headers=headers, verify=False, timeout=15)
        if r.status_code == 401:
            print("401 Unauthorized → forcing token refresh")
            _cached_tokens[base_url] = None
            headers["Authorization"] = f"Bearer {bearer_token(base_url)}"
            r = requests.get(url, headers=headers, verify=False, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if "401" in str(e):
            print("Still 401 after retry → trying secondary")
            return send_api_get_call(full_path, base_secondary)
        raise


def send_api_put_call(full_path, json_payload, base_url=base_primary):
    global _cached_tokens

    url = base_url + full_path
    headers = {
        "Authorization": f"Bearer {bearer_token(base_url)}",
        "Accept": "application/json"
    }

    try:
        r = requests.put(url, headers=headers, verify=False, timeout=15, json=json_payload)
        if r.status_code == 401:
            print("401 Unauthorized → forcing token refresh")
            _cached_tokens[base_url] = None
            headers["Authorization"] = f"Bearer {bearer_token(base_url)}"
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
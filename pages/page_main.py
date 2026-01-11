# Python Standard Packages
import streamlit as st
import re
import time
import os
import json
import time
import requests
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date, timezone
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Streamlit API
import folium
from streamlit_folium import st_folium

WEBAPP_NAME = "PowerSupplyOB"
# SLEEP_SEC_CHANGE_DATA = 0.05 # 데이터 입력 딜레이

# load_dotenv()

# Streamlit Folium
# import folium
# from streamlit_folium import st_folium

# Discord API
SLEEP_SEC_SEND_DM = 0.01
DISCORD_BOT_TOKEN = st.secrets["DISCORD_BOT_TOKEN"]
HEADERS = {
    "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
    "Content-Type": "application/json"
}

# Google Spreadsheet API
# from oauth2client.service_account import ServiceAccountCredentials
# import gspread
from streamlit_gsheets import GSheetsConnection
import gspread
from google.oauth2.service_account import Credentials

SLEEP_SEC_READ_SHEET = 0.03 # 구글 스프레드 시트
SLEEP_SEC_UPDATE_CELL = 0.01 # 구글 스프레드 시트
SLEEP_SEC_ADD_DATA = 0.01 # 구글 스프레드 시트

kst_now = datetime.now(timezone(timedelta(hours=9)))
today = kst_now.date()
today_yyyymmdd = today.strftime("%Y%m%d")
today_yyyymm = today.strftime("%Y%m")
today_yyyy = today.strftime("%Y")

def custom_rerun():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

def close_gc():
    try:
        gc = st.session_state["gc"]
        gc.auth.transport.close()
        del st.session_state["gc"]
    except:
        pass

def format_phone_number(phone):
    try:
        if isinstance(phone, float):
            phone = int(phone)
        phone_str = str(phone).replace('.0', '')
        if len(phone_str)==10 and phone_str.isdigit():
            return f"0{phone_str}"
        else:
            return phone_str
    except:
        return str(phone)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def get_sheet_instance(sheet_name):
    connection_info = st.secrets["connections"][sheet_name]
    service_account_info = {
        "type": connection_info["type"],
        "project_id": connection_info["project_id"],
        "private_key_id": connection_info["private_key_id"],
        "private_key": connection_info["private_key"],
        "client_email": connection_info["client_email"],
        "client_id": connection_info["client_id"],
        "auth_uri": connection_info["auth_uri"],
        "token_uri": connection_info["token_uri"],
        "auth_provider_x509_cert_url": connection_info["auth_provider_x509_cert_url"],
        "client_x509_cert_url": connection_info["client_x509_cert_url"]
    }
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(
        service_account_info, 
        scopes=scope
    )
    gc = gspread.authorize(credentials)
    st.session_state["gc"] = gc
    
    spreadsheet_url = connection_info["spreadsheet"]
    spreadsheet = gc.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    return worksheet    

# @st.cache_data(ttl=30) 
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def read_sheet(sheetname:str):
    """
    지정된 시트의 데이터를 읽어옵니다.
    """
    try:
        conn = st.connection(sheetname, type=GSheetsConnection, ttl=0)
        df = conn.read(worksheet=sheetname, ttl=0)
        df = df.copy()
        
        if "user_id" in df.columns:
            df["user_id"] = df["user_id"].astype(str).apply(lambda x: x.replace("mbr", ""))
        if "poll_id" in df.columns:
            df["poll_id"] = df["poll_id"].astype(str).apply(lambda x: x.replace("poll", ""))
        if "thread_id" in df.columns:
            df["thread_id"] = df["thread_id"].astype(str).apply(lambda x: x.replace("thread", ""))
        if 'phn_no' in df.columns:
            df['phn_no'] = df['phn_no'].apply(format_phone_number)
        keys = [
            "birth_date", "student_no", "zip_code", "due_date", "request_date", 
            "yearmonth", "poll_date", "deposit_date", "amount", "date_partition", 
            "mbr_cnt", "active_mbr_cnt", "warm_mbr_cnt", "attendant_mbr_cnt", "not_voted_mbr_cnt"
        ]
        for key in keys:
            if key in df.columns:
                df[key] = df[key].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        keys = ["lat", "lng"]
        for key in keys:
            if key in df.columns:
                df[key] = df[key].apply(lambda x: float(x) if pd.notnull(x) else None)        
        return df
    except Exception as e:
        return None
        
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def add_data(sheetname:str, df):
    """
    지정된 시트에 데이터를 추가합니다.
    """
    try:
        if "user_id" in df.columns:
            df['user_id'] = "mbr" + df['user_id'].astype(str)
        if "poll_id" in df.columns:
            df['poll_id'] = "poll" + df['poll_id'].astype(str)
        if "thread_id" in df.columns:
            df['thread_id'] = "thread" + df['thread_id'].astype(str)
        sheet = get_sheet_instance(sheetname)
        values = df.values.tolist()
        sheet.append_rows(values, value_input_option="RAW")
        time.sleep(SLEEP_SEC_ADD_DATA)
        close_gc()
        return True
    except Exception as e:
        close_gc()
        return False

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def update_cell(sheetname, cell, value): 
    """
    특정 셀 업데이트
    input: "시트1", "A1", "Hello World"
    """
    try:
        sheet = get_sheet_instance(sheetname)
        if sheet:
            sheet.update_acell(cell, value)
            time.sleep(SLEEP_SEC_UPDATE_CELL)
            close_gc()
            return True
    except Exception as e:
        close_gc()
        return False

def show_msg(key:str):
    prev_msg = st.session_state.get(key, (None, None))
    if prev_msg:
        msg_type, msg_text = prev_msg
        if msg_type=="success" and msg_text is not None:
            st.success(msg_text)
        elif msg_type=="warning" and msg_text is not None:
            st.warning(msg_text)
        elif msg_type=="error" and msg_text is not None:
            st.error(msg_text)
        elif msg_type=="info" and msg_text is not None:
            st.info(msg_text)
        st.session_state[key] = (None, None)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def send_dm(user_id:str, server_nick:str, msg:str):
    """
    특정 유저에게 DM을 전송하는 함수입니다.
    """
    try:
        # DM 채널 개설
        dm_url  = "https://discord.com/api/v10/users/@me/channels"
        dm_data = {"recipient_id": user_id}
        HEADERS = {
            "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
            "Content-Type": "application/json"
        }
        dm_res  = requests.post(dm_url, headers=HEADERS, json=dm_data)
        dm_res.raise_for_status()
        channel_id = dm_res.json()["id"]

        # DM 전송
        msg_url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        msg_data = {"content": msg}
        response  = requests.post(msg_url, headers=HEADERS, json=msg_data)
        response.raise_for_status()
        time.sleep(SLEEP_SEC_SEND_DM)

        if response.status_code in [200, 201]:
            return None
        elif response.status_code==403 and response.json().get("code")==50007:
            return {"user_id": user_id, "server_nick": server_nick}
        else:
            return None
    except Exception as e:
        return None

def menu_dashboard():
    """
    대시보드 메뉴
    """
    menu_items_dashboard = ["회원 상태", "지표", "회원 현황"]
    selected_menu_dashboard = st.selectbox(
        label="하위 메뉴",
        options=menu_items_dashboard,
        key="dashboard_menu_select"
    )
    st.markdown("---")

    if selected_menu_dashboard==menu_items_dashboard[0]:
        # 데이터 로드
        mbr_df = read_sheet("tbl_mbr_inf_snp")
        dormant_df = read_sheet("tbl_mbr_dormant_his")

        st.markdown("##### 회원 상태")
        user_id = st.session_state.get("user_id", "")
        mbr_user_df = mbr_df[mbr_df["user_id"]==user_id].reset_index(drop=True)
        status = mbr_user_df["active_yn"].iloc[0]
        if status == "y":
            status = "활성"
        else:
            status = "휴면"
        st.markdown(f"- 상태: {status}")

        due_date_ym = mbr_user_df["due_date"].iloc[0].replace("'", "")
        st.markdown(f"- 회비 유효기한: {due_date_ym}")
        
        cond1 = dormant_df["user_id"]==user_id
        cond2 = dormant_df["dormant_yn"]=="y"
        cond3 = dormant_df["dormant_admin_yn"]=="y"
        cond4 = dormant_df["valid_yn"]=="y"
        dormant_df = dormant_df[cond1&cond2&cond3&cond4].reset_index(drop=True)
        # 휴면으로 등록된 기간 리스트
        dormant_ym_lst = dormant_df["yearmonth"].tolist()

        def get_ym_lst(start_ym:str, end_ym:str):
            ym_lst = []
            curr = datetime.strptime(start_ym, "%Y%m")
            end = datetime.strptime(end_ym, "%Y%m")
            while curr <= end:
                ym_lst.append(curr.strftime("%Y%m"))
                curr += relativedelta(months=1)
            return ym_lst

        if today_yyyymm >= due_date_ym: # 유효기한이 과거/현재인 경우: 유효기한 ~ 현재
            ym_lst = get_ym_lst(due_date_ym, today_yyyymm)            
            status_lst = []
            for ym in ym_lst:
                if ym in dormant_ym_lst:
                    status_lst.append("휴면")
                    continue
                if ym <= due_date_ym:
                    status_lst.append("납부")
                else:
                    status_lst.append("미납")
            disp1_df = pd.DataFrame({
                "yearmonth": ym_lst,
                "dormant_yn": status_lst
            })
        else:
            ym_lst = get_ym_lst(today_yyyymm, due_date_ym)
            status_lst = []
            for ym in ym_lst:
                if ym in dormant_ym_lst:
                    status_lst.append("휴면")
                    continue
                if ym <= due_date_ym:
                    status_lst.append("납부")
                else:
                    status_lst.append("미납")
            disp1_df = pd.DataFrame({
                "yearmonth": ym_lst,
                "dormant_yn": status_lst
            })
        disp1_df.columns = ["기간", "상태"]
        st.dataframe(
            disp1_df, 
            column_config={
                "기간": st.column_config.TextColumn(
                    "기간",
                    help="유효기한 및 현시점을 기준으로 정렬됩니다.",
                ),
                "상태": st.column_config.TextColumn(
                    "상태",
                    help="납부/미납/휴면 중 하나로 표현됩니다.",
                ),
            },
            hide_index=True, 
            width="stretch"
        )
    elif selected_menu_dashboard==menu_items_dashboard[1]:
        # 데이터 로드
        idx_df = read_sheet("tbl_dashboard_index_his")

        st.markdown("##### 지표")

        idx_6m_df = idx_df.tail(24).reset_index(drop=True) # 최근 6개월만 표시
        idx_2w_df = idx_6m_df.tail(2).reset_index(drop=True)

        # 최근 2주 데이터 파싱
        def get_val(idx, col, default=0):
            return int(idx_2w_df.loc[idx, col]) if len(idx_df) > idx else default

        past_mbr_cnt = get_val(0, "mbr_cnt")
        current_mbr_cnt = get_val(1, "mbr_cnt", get_val(0, "mbr_cnt"))

        past_active_mbr_cnt = get_val(0, "active_mbr_cnt")
        current_active_mbr_cnt = get_val(1, "active_mbr_cnt", get_val(0, "active_mbr_cnt"))

        past_warm_mbr_cnt = get_val(0, "warm_mbr_cnt")
        current_warm_mbr_cnt = get_val(1, "warm_mbr_cnt", get_val(0, "warm_mbr_cnt"))

        past_attendant_mbr_cnt = get_val(0, "attendant_mbr_cnt")
        current_attendant_mbr_cnt = get_val(1, "attendant_mbr_cnt", get_val(0, "attendant_mbr_cnt"))

        past_not_voted_mbr_cnt = get_val(0, "not_voted_mbr_cnt")
        current_not_voted_mbr_cnt = get_val(1, "not_voted_mbr_cnt", get_val(0, "not_voted_mbr_cnt"))

        past_workout_yn = idx_2w_df.loc[0, "workout_yn"] if len(idx_2w_df) > 0 else 'n'
        current_workout_yn = idx_2w_df.loc[1, "workout_yn"] if len(idx_2w_df) > 1 else (idx_df.loc[0, "workout_yn"] if len(idx_df) > 0 else 'n')
        past_workout_cnt = 1 if past_workout_yn == 'y' else 0
        current_workout_cnt = 1 if current_workout_yn == 'y' else 0
        
        # 지표 표시
        row1 = st.container(horizontal=True)
        with row1:
            [col1, col2] = st.columns(2)
            with col1:
                st.metric(
                    label="전체 회원", value=f"{current_mbr_cnt}명",
                    delta=f"{current_mbr_cnt - past_mbr_cnt}명",
                    border=True,
                )
            with col2:
                st.metric(
                    label="활성 회원", value=f"{current_active_mbr_cnt}명",
                    delta=f"{current_active_mbr_cnt - past_active_mbr_cnt}명",
                    border=True,
                )
        row2 = st.container(horizontal=True)
        with row2:
            [col1, col2] = st.columns(2)
            with col1:
                st.metric(
                    label="참석 회원(최근 4주)", value=f"{current_warm_mbr_cnt}명",
                    delta=f"{current_warm_mbr_cnt - past_warm_mbr_cnt}명",
                    chart_data=idx_6m_df.warm_mbr_cnt.tolist(),
                    chart_type="bar", border = True,
                    help="최근 4주 내 1회 이상 참석 기준"
                )
            with col2:
                st.metric(
                    label="참석 회원(최근 1주)", value=f"{current_attendant_mbr_cnt}명",
                    delta=f"{current_attendant_mbr_cnt - past_attendant_mbr_cnt}명",
                    chart_data=idx_6m_df.attendant_mbr_cnt.tolist(),
                    chart_type="bar", border = True,
                    help="최근 투표 참석 기준"
                )
        row3 = st.container(horizontal=True)
        with row3:
            [col1, col2] = st.columns(2)
            with col1:
                st.metric(
                    label="미투표 회원", value=f"{current_not_voted_mbr_cnt}명",
                    delta=f"{current_not_voted_mbr_cnt - past_not_voted_mbr_cnt}명",
                    chart_data=idx_6m_df.not_voted_mbr_cnt.tolist(),
                    chart_type="bar", border = True,
                    help="최근 투표 미투표자"
                )
            with col2:
                st.metric(
                    label="운동 진행 횟수", value=f"{current_workout_cnt}회",
                    delta=f"{current_workout_cnt - past_workout_cnt}회",
                    chart_data=[1 if yn=="y" else 0 for yn in idx_6m_df.workout_yn.tolist()],
                    chart_type="bar", border = True,
                    help="최근 6개월 기준"
                )
    elif selected_menu_dashboard==menu_items_dashboard[2]:
        # 데이터 로드
        mbr_df = read_sheet("tbl_mbr_inf_snp")
        st.markdown("##### 회원 명단")
        st.markdown("- 본인 정보 수정:[구글 설문](https://forms.gle/bWi1aLa7Pgh4F2Wz9)에서 기존 응답한 이메일로 재응답해주세요.")
        st.markdown("- 차주 월 12시 이후 반영됩니다.")

        # 회원 명단 테이블
        disp_mbr_df = mbr_df[["server_nick", "active_yn", "due_date", "phn_no", "mail"]].copy()
        disp_mbr_df["active_yn"] = disp_mbr_df["active_yn"].map({"y": "활성", "n": "휴면"}).fillna(disp_mbr_df["active_yn"])
        disp_mbr_df = disp_mbr_df.sort_values(by="server_nick", ascending=True).reset_index(drop=True)
        disp_mbr_df.columns = ["이름", "상태", "회비유효기한", "연락처", "이메일"]
        st.dataframe(disp_mbr_df, hide_index=True, width="stretch")

        # 지도 표시
        st.markdown("##### 회원 위치 현황")
        st.markdown("- Red: 개인 위치 / Blue: 평균 위치")
        st.markdown("- 확대할 수 있는 범위는 제한됩니다.")
        coord_df = mbr_df[["lat", "lng"]].copy()
        coord_df = coord_df[(coord_df["lat"] != "") & (coord_df["lng"] != "")].reset_index(drop=True)
        coord_df = coord_df[(coord_df["lat"].notnull()) & (coord_df["lng"].notnull())].reset_index(drop=True)

        if len(coord_df) > 0:
            lat_avg = coord_df["lat"].astype(float).mean()
            lng_avg = coord_df["lng"].astype(float).mean()
            map = folium.Map(location=[lat_avg, lng_avg], zoom_start=10, min_zoom=3, max_zoom=12)
            for _, row in coord_df.iterrows():
                folium.CircleMarker(
                    location=[float(row["lat"]), float(row["lng"])],
                    radius=20, color='red', fill=True, fill_color='#EC4074'
                ).add_to(map)

            folium.CircleMarker(
                location=[lat_avg, lng_avg],
                radius=25, color='blue',fill=True, fill_color='blue', fill_opacity=0.7, popup='mean'
            ).add_to(map)
            st_folium(map, height=400, use_container_width=True, returned_objects=[])

def menu_charge_req():
    """정산요청 메뉴"""
    # 하위 메뉴
    menu_items_charge_req = ["회비", "벌금", "경비"]
    selected_menu_charge_req = st.selectbox(
        label="하위 메뉴",
        options=menu_items_charge_req,
        key="charge_req_menu_select"
    )
    st.markdown("---")

    request_date = today_yyyymmdd
    user_id = st.session_state.get("user_id", "")
    server_nick = st.session_state.get("server_nick", "")
    user_check_yn = "y"
    admin_check_yn = "n"

    # 회비 정산
    if selected_menu_charge_req==menu_items_charge_req[0]:
        # 현재 회원 납부 기간
        mbr_df = read_sheet("tbl_mbr_inf_snp")    
        due_date = mbr_df[mbr_df["user_id"] == user_id]["due_date"].values[0]
        due_date = str(due_date).replace("'", "")
        st.markdown("##### 회비 정산")
        st.markdown("- 합계 금액 입금 후 요청해주세요.")
        st.markdown(f"- 현재 회비 유효 기한: {due_date[:4]}/{due_date[4:]}")
        st.markdown(f"- 1개월당 회비: 15,000원")
        st.markdown(f"- 12개월 회비: 170,000원 (할인 10,000원)")
        
        charge_req_btn1 = False
        with st.form(key="charge_req_form1"):
            # 기본 정보
            # 1. 본인 입금일
            deposit_date = st.date_input(
                "입금일", value=today, max_value="today",
                help="본인이 입금한 날짜를 정확히 입력해주세요.",
                key="deposit_date1"
            )
            deposit_date = str(deposit_date).replace("-", "")
            # 2. 납부 기간 입력
            month_cnt = st.number_input(
                "납부 기간(개월)", 
                value=1, min_value=1, max_value=12, step=1,
                help="납부한 회비 기간의 개월 수를 입력해주세요. (1~12개월)",
                key="month_cnt",
                on_change=None  # 값이 바뀔 때 리런 방지
            )
            amount = 170000 if month_cnt == 12 else 15000 * month_cnt
            charge_req_btn1 = st.form_submit_button("요청", key="charge_req_btn1", use_container_width=True)
            if charge_req_btn1:
                with st.spinner(f"In progress...", show_time=True):
                    if month_cnt>=1:
                        df = pd.DataFrame([{
                            "request_date": request_date,
                            "user_id": user_id,
                            "server_nick": server_nick,
                            "charge_type": "회비",
                            "charge_detail": f"회비 {month_cnt}개월",
                            "deposit_date": deposit_date,
                            "amount": amount,
                            "user_check_yn": user_check_yn,
                            "admin_check_yn": admin_check_yn,
                            "valid_yn": "y"
                        }])
                        add_data("tbl_charge_inf_his", df)
                        st.session_state["charge_req_msg1"] = ("success", f"요청 완료되었습니다. {month_cnt}개월 {amount:,}원")
                        custom_rerun()
                    else:
                        st.session_state["charge_req_msg1"] = ("warning", "납부기간을 입력해주세요.")
            show_msg("charge_req_msg1")

    # 벌금 정산
    elif selected_menu_charge_req==menu_items_charge_req[1]:
        # 정산 내역 테이블
        charge_df = read_sheet("tbl_charge_inf_his")
        charge_df["idx"] = charge_df.index

        # 항목별 횟수 입력       
        if len(charge_df)>0:
            cond1 = charge_df["charge_type"] == "벌금"
            cond2 = charge_df["charge_detail"].str.contains("미투표")
            cond3 = charge_df["user_id"] == user_id
            cond4 = charge_df["user_check_yn"] == "n"
            cond5 = charge_df["valid_yn"] == "y"
            notvote_charge_df = charge_df[cond1&cond2&cond3&cond4&cond5].reset_index(drop=True)
            notvote_charge_df = notvote_charge_df.sort_values(by="request_date", ascending=True).reset_index(drop=True)
            not_voted_cnt = len(notvote_charge_df)
        else:
            notvote_charge_df = charge_df.copy()
            not_voted_cnt = 0

        st.markdown("##### 벌금 정산")
        st.markdown("- 합계 금액 입금 후 요청해주세요.")
        st.markdown("- 미투표/지각: 5,000원/회")
        st.markdown("- 불참: 20,000원/회")
        
        charge_req_btn2 = False
        with st.form(key="charge_req_form2"):
            # 입금일 입력
            deposit_date = st.date_input(
                "입금일", value=today,
                help="본인이 입금한 날짜를 정확히 입력해주세요.",
                key="deposit_date2"
            )
            deposit_date = str(deposit_date).replace("-", "")
            # 벌금 횟수 입력
            charge_input1 = st.number_input(
                "미투표(회)", value=not_voted_cnt, min_value=0, max_value=not_voted_cnt, step=1,
                key="charge_input1",
                help="미투표 횟수를 입력해주세요. 설정된 기본 값은 모니터링된 과거 미투표 내역의 횟수입니다. 기존 내역보다 낮은 횟수를 입력할 때, 과거 데이터부터 정산 요청됩니다."
            )
            charge_input2 = st.number_input(
                "지각(회)", value=0, min_value=0, step=1, 
                key="charge_input2",
                help="지각 횟수를 입력해주세요."
            )
            charge_input3 = st.number_input(
                "불참(회)", value=0, min_value=0, step=1,
                key="charge_input3",
                help="불참 횟수를 입력해주세요."
            )
            total_amount = charge_input1*5000 + charge_input2*5000 + charge_input3*20000
            charge_req_btn2 = st.form_submit_button("요청", key="charge_req_btn2", use_container_width=True)
            if charge_req_btn2:
                with st.spinner(f"In progress...", show_time=True):
                    if total_amount>0:
                        # 미투표 정산 요청 반영
                        if charge_input1>=1 and charge_input1 is not None:
                            notvote_charge_df = notvote_charge_df.head(charge_input1).copy()
                            for _, row in notvote_charge_df.iterrows():
                                idx = row["idx"]
                                # deposit_date 업데이트
                                update_cell("tbl_charge_inf_his", f"F{idx+2}", "'"+deposit_date)
                                # user_check_yn을 y로 변경
                                update_cell("tbl_charge_inf_his", f"H{idx+2}", "y")
                        # 지각, 불참 정산 요청 반영
                        if charge_input2>=1 or charge_input3>=1:
                            df = pd.DataFrame({
                                "request_date": [request_date]*2,
                                "user_id": [user_id]*2,
                                "server_nick": [server_nick]*2,
                                "charge_type":["벌금"]*2,
                                "charge_detail": [f"지각 {charge_input2}회", f"불참 {charge_input3}회"],  
                                "deposit_date": [deposit_date]*2,
                                "amount": [charge_input2*5000, charge_input3*20000],
                                "user_check_yn": [user_check_yn]*2,
                                "admin_check_yn": [admin_check_yn]*2,
                                "valid_yn": ["y"]*2
                            })
                            df = df[(df["amount"]!=0) & (df["amount"].notnull())].reset_index(drop=True)
                            if len(df) > 0:
                                add_data("tbl_charge_inf_his", df)
                        st.session_state["charge_req_msg2"] = ("success", f"요청 완료되었습니다. 합계 금액: {total_amount:,}원")
                        custom_rerun()
                    else:
                        st.session_state["charge_req_msg2"] = ("warning", "요청할 데이터가 없습니다.")
            show_msg("charge_req_msg2")

        # 과거 미투표 내역
        st.markdown("##### [참고] 과거 미투표 내역")
        st.markdown("운영진의 반려 시, 요청한 내용이 다시 보여집니다.")
        raw_df = notvote_charge_df.copy()
        raw_df.columns = [
            "이름" if col == "server_nick" else
            "상세" if col == "charge_detail" else
            "금액" if col == "amount" else col
            for col in raw_df.columns
        ]
        raw_df = raw_df[["이름", "상세", "금액"]]
        st.dataframe(raw_df, hide_index=True, width="stretch")

    # 경비 정산
    elif selected_menu_charge_req==menu_items_charge_req[2]:
        poll_df = read_sheet("tbl_poll_inf_his")
        
        st.markdown("##### 경비 정산")
        st.markdown("- 운동일자, 항목별 금액을 입력하세요.")
        st.markdown("- 주차: 최대 5,000원")
        
        charge_req_btn3 = False
        with st.form(key="charge_req_form3"):
            # 최근 4주 중 운동일정 선택
            workout_date_lst = sorted(poll_df["poll_date"].unique().tolist(), reverse=True)[:4]
            workout_date = st.selectbox(
                "운동일정 선택",
                options=workout_date_lst,
                help="최근 4주 중 운동일정을 선택해주세요.",
                key="event_date_select1"
            )
            fee_input1 = st.number_input(
                "구장(원)",
                value=0, min_value=0, step=10,
                key="fee_input1",
                help="구장 예약 시 발생한 비용을 입력하세요."
            )
            fee_input2 = st.number_input(
                "음료(원)",
                value=0, min_value=0, step=10,
                key="fee_input2",
                help="발생한 음료 구매 금액을 입력하세요."
            )
            fee_input3 = st.number_input(
                "주차(원)",
                value=0, min_value=0, max_value=5000, step=10,
                key="fee_input3",
                help="발생한 주차비를 입력하세요. (최대 5,000원)"
            )
            total_amount = fee_input1 + fee_input2 + fee_input3
            charge_req_btn3 = st.form_submit_button("요청", key="charge_req_btn3", use_container_width=True)
            if charge_req_btn3:
                with st.spinner(f"In progress...", show_time=True):
                    if workout_date is None:
                        st.session_state["charge_req_msg3"] = ("warning", "운동 일정을 선택해주세요.")
                    else:
                        if total_amount>0 and fee_input3>5000:
                            st.session_state["charge_req_msg3"] = ("warning", "주차의 경우 최대 5,000원까지 요청 가능합니다.")
                        elif total_amount>0 and fee_input3<=5000:
                            df = pd.DataFrame({
                                "request_date": [request_date]*3,
                                "user_id": [user_id]*3,
                                "server_nick": [server_nick]*3,
                                "charge_type":["구장", "음료", "주차"],
                                "charge_detail": [f"{workout_date}(일) 구장", f"{workout_date}(일) 음료", f"{workout_date}(일) 주차"],  
                                "deposit_date": [None]*3,
                                "amount": [fee_input1, fee_input2, fee_input3],
                                "user_check_yn": [user_check_yn]*3,
                                "admin_check_yn": [admin_check_yn]*3,
                                "valid_yn": ["y"]*3
                            })
                            df = df[df["amount"].notnull() & (df["amount"]!=0)].reset_index(drop=True)
                            if len(df) > 0:
                                add_data("tbl_charge_inf_his", df)
                                st.session_state["charge_req_msg3"] = ("success", f"요청 완료되었습니다. 합계 금액: {total_amount:,}원")
                                custom_rerun()
                            else:
                                st.session_state["charge_req_msg3"] = ("warning", "요청할 데이터가 없습니다.")
                        else:
                            st.session_state["charge_req_msg3"] = ("warning", "요청할 데이터가 없습니다.")
            show_msg("charge_req_msg3")

def menu_dormant_request():
    """휴면요청 메뉴"""
    dormant_df = read_sheet("tbl_mbr_dormant_his")
    dormant_df["idx"] = dormant_df.index
    
    menu_items_dormant_req = ["휴면 신청", "휴면 철회"]
    selected_menu_dormant_req = st.selectbox(
        label="하위 메뉴",
        options=menu_items_dormant_req,
        key="dormant_req_menu_select"
    )
    st.markdown("---")

    user_id = st.session_state.get("user_id", "")
    user_nick = st.session_state.get("server_nick", "Unknown")

    # 휴면 신청
    if selected_menu_dormant_req==menu_items_dormant_req[0]:
        st.markdown("##### 휴면 신청")
        with st.form(key="request_dormant_form_apply"):
            # 요청 날짜 연월 기준 향후 12개월
            ym_list = [
                (today + relativedelta(months=+i)).strftime("%Y%m")
                for i in range(12)
            ]
            [start_ym, end_ym] = st.select_slider(
                "적용 기간(연월)을 선택하세요.",
                options=ym_list, value=(ym_list[0], ym_list[1]),
                format_func=lambda x: f"{x[:4]}/{x[4:]}",
                key="dormant_period_slider"
            )

            dormant_req_btn1 = st.form_submit_button("요청", key="dormant_req_btn1", use_container_width=True)
            if dormant_req_btn1:
                with st.spinner(f"In progress...", show_time=True):
                    cond1 = dormant_df["user_id"]==user_id
                    cond2 = dormant_df["dormant_yn"]=="y"
                    cond3 = dormant_df["valid_yn"]=="y"
                    user_df = dormant_df[cond1&cond2&cond3].reset_index(drop=True)

                    ym_start = datetime.strptime(start_ym, "%Y%m")
                    ym_end = datetime.strptime(end_ym, "%Y%m")
                    yearmonth_lst = []
                    cur_ym = ym_start
                    while cur_ym <= ym_end:
                        yearmonth_lst.append(cur_ym.strftime("%Y%m"))
                        cur_ym += relativedelta(months=1)

                    df = pd.DataFrame({
                        "request_date": today_yyyymmdd,
                        "user_id": user_id,
                        "server_nick": user_nick,
                        "yearmonth": yearmonth_lst,
                        "dormant_yn": "y",
                        "dormant_admin_yn": "y",
                        "dormant_admin_yn": "n",
                        "withdrawal_yn": "n",
                        "withdrawal_admin_yn": "n",
                        "valid_yn": "y"
                    })
                    if len(df) >= 1:
                        exist_yearmonth_lst = user_df["yearmonth"].unique().tolist()
                        if set(yearmonth_lst) & set(exist_yearmonth_lst):
                            st.session_state["dormant_req_msg1"] = ("warning", "기존 신청과 중복된 기간이 있습니다.")
                        else:
                            add_data("tbl_mbr_dormant_his", df)
                            st.session_state["dormant_req_msg1"] = ("success", "요청 완료되었습니다.")
                            custom_rerun()
                    else:
                        st.session_state["dormant_req_msg1"] = ("warning", "요청할 데이터가 없습니다.")
            show_msg("dormant_req_msg1")
    # 휴면 철회
    elif selected_menu_dormant_req==menu_items_dormant_req[1]:
        cond1 = dormant_df["user_id"]==user_id
        cond2 = dormant_df["dormant_yn"]=="y"
        cond3 = dormant_df["dormant_admin_yn"]=="y"
        cond4 = dormant_df["withdrawal_yn"]=="n"
        cond5 = dormant_df["withdrawal_admin_yn"]=="n"
        cond6 = dormant_df["valid_yn"]=="y"
        user_df = dormant_df[cond1&cond2&cond3&cond4&cond5&cond6].reset_index(drop=True)
        user_df["select_yn"] = False
        st.markdown("##### 휴면 철회")
        with st.form(key="request_dormant_form_cancel"):
            st.markdown("운영진이 승인한 휴면 요청 목록입니다.")
            edit_df = st.data_editor(
                user_df,
                column_config={
                    "select_yn": st.column_config.CheckboxColumn("선택", disabled=False, default=False),
                    "yearmonth": st.column_config.TextColumn("대상 기간", disabled=True)
                },
                column_order=["select_yn", "yearmonth"],
                num_rows="fixed",
                hide_index=True,
                width="stretch",
                key="dormant_cancellation_editor"
            )
            dormant_req_btn2 = st.form_submit_button("요청", key="dormant_req_btn2", use_container_width=True)
            if dormant_req_btn2:
                with st.spinner(f"In progress...", show_time=True):
                    selected_df = edit_df[edit_df["select_yn"]==True].reset_index(drop=True)
                    if len(selected_df)>=1:
                        for _, row in selected_df.iterrows():
                            idx = row["idx"]
                            # request_date 업데이트
                            update_cell("tbl_mbr_dormant_his", f"A{idx+2}", "'"+today_yyyymmdd)
                            # withdrawal_yn 업데이트
                            update_cell("tbl_mbr_dormant_his", f"G{idx+2}", "y")
                        st.session_state["dormant_req_msg2"] = ("success", "요청 완료되었습니다.")
                        custom_rerun()
                    else:
                        st.session_state["dormant_req_msg2"] = ("warning", "요청할 데이터가 없습니다.")
            show_msg("dormant_req_msg2")

    # 과거 휴면 내역
    st.markdown("##### [참고] 현재 휴면 내역")
    st.markdown("- 요청 중 또는 운영진이 승인한 휴면 신청/철회 내역입니다.")
    cond1 = dormant_df["user_id"]==user_id
    cond2 = dormant_df["dormant_yn"]=="y"
    cond3 = dormant_df["withdrawal_yn"]=="n"
    cond4 = dormant_df["withdrawal_admin_yn"]=="n"
    cond5 = dormant_df["valid_yn"]=="y"    
    disp_df = dormant_df[cond1&cond2&cond3&cond4&cond5].reset_index(drop=True)
    disp_df.columns = [
        "요청일자" if col == "request_date" else
        "휴면기간" if col == "yearmonth" else
        "상태" if col == "withdrawal_admin_yn" else col
        for col in disp_df.columns
    ]
    disp_df = disp_df[["요청일자", "휴면기간", "상태"]]
    disp_df["상태"] = disp_df["상태"].map({"y": "승인", "n": "요청"}).fillna(disp_df["상태"])
    st.dataframe(disp_df, hide_index=True, width="stretch")

def menu_request_status():
    """요청 현황 메뉴"""
    # 데이터 로드
    charge_df = read_sheet("tbl_charge_inf_his")
    charge_df["idx"] = charge_df.index
    dormant_df = read_sheet("tbl_mbr_dormant_his")
    dormant_df["idx"] = dormant_df.index

    user_id = st.session_state.get("user_id", "")

    st.markdown("---")
    st.markdown("##### 1. 정산 요청 현황")

    with st.form(key="request_status_form1"):
        cond1 = charge_df["user_id"]==user_id
        cond2 = charge_df["user_check_yn"]=="y"
        cond3 = charge_df["admin_check_yn"]=="n"
        cond4 = charge_df["valid_yn"] == "y"
        df1 = charge_df[cond1&cond2&cond3&cond4].reset_index(drop=True)
        df1['select_yn'] = False
        df1["deposit_date"] = df1["deposit_date"].astype(str) # 
        edit_df1 = st.data_editor(
            df1,
            column_config={
                "select_yn": st.column_config.CheckboxColumn("선택", disabled=False, default=False),
                "charge_type": st.column_config.TextColumn("유형", disabled=True),
                "charge_detail": st.column_config.TextColumn("상세", disabled=True),
                "deposit_date": st.column_config.TextColumn("입금일", disabled=False),
                "amount": st.column_config.NumberColumn("금액", disabled=True),
            },
            column_order=["select_yn", "charge_type", "charge_detail", "deposit_date", "amount"],
            num_rows="fixed",
            hide_index=True,
            width="stretch",
        )
        req_cancel_btn1 = st.form_submit_button("요청 취소", key="req_cancel_btn1", width="stretch")
        if req_cancel_btn1:
            with st.spinner(f"In progress...", show_time=True):
                edit_df1["select_yn"] = edit_df1["select_yn"].apply(lambda x: "y" if x else "n")
                edit_df1 = edit_df1[edit_df1["select_yn"]=="y"].reset_index(drop=True)
                if len(edit_df1)>=1:
                    # 미투표 벌금 -> user_check_yn을 n으로 변경 / deposit_date 셀값 삭제
                    cond1 = edit_df1["charge_type"]=="벌금"
                    cond2 = edit_df1["charge_detail"].str.contains("미투표")
                    idx1_lst = edit_df1[cond1&cond2]["idx"].unique().tolist()
                    for idx in idx1_lst:
                        # user_check_yn 변경: y->n
                        update_cell("tbl_charge_inf_his", f"H{idx+2}", "n")
                        # deposit_date 셀값 삭제
                        update_cell("tbl_charge_inf_his", f"F{idx+2}", "")
                    
                    # 나머지 정산 -> valid_yn을 n으로 변경
                    cond1 = ~edit_df1["charge_detail"].str.contains("미투표")
                    idx2_lst = edit_df1[cond1]["idx"].unique().tolist()
                    for idx in idx2_lst:
                        # valid_yn 업데이트
                        update_cell("tbl_charge_inf_his", f"J{idx+2}", "n")
                    st.session_state["req_status_msg1"] = ("success", "요청 취소되었습니다.")
                    custom_rerun()
                else:
                    st.session_state["req_status_msg1"] = ("warning", "요청 취소할 데이터가 없습니다.")
        show_msg("req_status_msg1")

    st.markdown("##### 2. 휴면 요청 현황")
    with st.form(key="request_status_form2"):
        cond1 = dormant_df["user_id"]==user_id
        cond2 = dormant_df["dormant_yn"]=="y"
        cond3 = dormant_df["withdrawal_admin_yn"]=="n"
        cond4 = dormant_df["valid_yn"] == "y"
        df2 = dormant_df[cond1&cond2&cond3&cond4].reset_index(drop=True)
        df2['cancel_yn'] = False
        df2["dormant_admin_yn"] = df2["dormant_admin_yn"].map({"y": "철회", "n": "신청"}).fillna(df2["dormant_admin_yn"])
        edit_df2 = st.data_editor(
            df2,
            column_config={
                "cancel_yn": st.column_config.CheckboxColumn("선택", disabled=False, default=False),
                "yearmonth": st.column_config.TextColumn("대상 기간", disabled=True),
                "dormant_admin_yn": st.column_config.TextColumn("요청 유형", disabled=True),
            },
            column_order=["cancel_yn", "yearmonth", "dormant_admin_yn"],
            num_rows="fixed",
            hide_index=True,
            width="stretch",
        )
        req_cancel_btn2 = st.form_submit_button("요청 취소", key="req_cancel_btn2", width="stretch")
        if req_cancel_btn2:
            with st.spinner(f"In progress...", show_time=True):
                edit_df2["cancel_yn"] = edit_df2["cancel_yn"].apply(lambda x: "y" if x else "n")
                edit_df2 = edit_df2[edit_df2["cancel_yn"]=="y"].reset_index(drop=True)
                if len(edit_df2)>=1:
                    # 요청 취소: valid_yn을 n으로 변경
                    idx_lst = edit_df2["idx"].unique().tolist()
                    for idx in idx_lst:
                        # valid_yn 업데이트
                        update_cell("tbl_mbr_dormant_his", f"I{idx+2}", "n")
                    st.session_state["req_status_msg2"] = ("success", "요청 취소되었습니다.")
                    custom_rerun()
                else:
                    st.session_state["req_status_msg2"] = ("warning", "요청 취소할 데이터가 없습니다.")
        show_msg("req_status_msg2")
        
def menu_admin_approval():
    """관리자 승인 메뉴"""
    # 데이터 로드
    charge_df = read_sheet("tbl_charge_inf_his")
    charge_df["idx"] = charge_df.index
    dormant_df = read_sheet("tbl_mbr_dormant_his")
    dormant_df["idx"] = dormant_df.index
    mbr_df = read_sheet("tbl_mbr_inf_snp")
    mbr_df["idx"] = mbr_df.index
    user_id = st.session_state.get("user_id", "")

    st.markdown("---")
    st.markdown("##### 1. 정산 요청 현황")
    approval_btn1, reject_btn1 = False, False
    with st.form(key="req_approval_form1"):
        cond1 = charge_df["user_check_yn"]=="y"
        cond2 = charge_df["admin_check_yn"]=="n"
        cond3 = charge_df["valid_yn"]=="y"
        charge_df = charge_df[cond1&cond2&cond3].reset_index(drop=True)
        charge_df["select_yn"] = False
        edit_df = st.data_editor(
            charge_df,
            column_config={
                "select_yn": st.column_config.CheckboxColumn("선택", disabled=False),
                "server_nick": st.column_config.TextColumn("이름", disabled=True),
                "charge_type": st.column_config.TextColumn("유형", disabled=True),
                "deposit_date": st.column_config.TextColumn("입금일", disabled=True),
                "amount": st.column_config.NumberColumn("금액", disabled=True),
                "charge_detail": st.column_config.TextColumn("상세", disabled=True),
            },
            column_order=["select_yn", "server_nick", "charge_type", "deposit_date", "amount", "charge_detail"],
            num_rows="fixed",
            hide_index=True,
            width="stretch",
            key="admin_approval_editor"
        )
        col1, col2 = st.columns([1, 1])
        with col1:
            approval_btn1 = st.form_submit_button("승인", key="approval_btn1", width="stretch")
            if approval_btn1:
                with st.spinner(f"In progress...", show_time=True):
                    edit_df["deposit_date"] = edit_df["deposit_date"].replace('', today_yyyymmdd)
                    edit_df["deposit_date"] = edit_df["deposit_date"].fillna(today_yyyymmdd)
                    edit_df["select_yn"] = edit_df["select_yn"].apply(lambda x: "y" if x else "n")
                    selected_df = edit_df[edit_df["select_yn"] == "y"].reset_index(drop=True)
                    if len(selected_df) >= 1:
                        # admin_check_yn 셀값 변경: n->y
                        for _, row in selected_df.iterrows():
                            idx = row["idx"]
                            update_cell("tbl_charge_inf_his", f"I{idx+2}", "y")

                        fee_df = selected_df[selected_df.charge_type == "회비"][["user_id", "charge_detail"]].reset_index(drop=True)
                        fee_df["month_cnt"] = fee_df["charge_detail"].str.extract(r"(\d+)").astype(int)
                        fee_df = fee_df[["user_id", "month_cnt"]].reset_index(drop=True)
                        user_ids = fee_df["user_id"].unique().tolist()

                        # due_date 업데이트
                        for idx, row in mbr_df.iterrows():
                            if row["user_id"] in user_ids:
                                due_date = row["due_date"]
                                add_month = fee_df[fee_df["user_id"] == row["user_id"]]["month_cnt"].sum()
                                new_due_date = (datetime.strptime(due_date, "%Y%m") + relativedelta(months=add_month)).strftime("%Y%m")
                                update_cell("tbl_mbr_inf_snp", f"M{idx+2}", "'" + new_due_date)

                        # DM 발송
                        user_info_df = selected_df[["user_id", "server_nick"]].drop_duplicates().reset_index(drop=True)
                        for idx, row in user_info_df.iterrows():
                            user_id = row["user_id"]
                            server_nick = row["server_nick"]
                            user_charge_df = selected_df[selected_df["user_id"]==user_id][["request_date", "charge_type", "charge_detail", "amount"]].reset_index(drop=True)
                            user_charge_df["dm_content"] = user_charge_df.apply(lambda x: f"- {x['request_date']} / {x['charge_type']} / {x['charge_detail']} / {x['amount']}", axis=1)
                            dm_content_lst = user_charge_df["dm_content"].tolist()
                            dm_content = "\n".join(dm_content_lst)
                            msg = f"""## [DM] 정산 요청 결과 알림(승인)
🎈안녕하세요. {server_nick}님.
아래 정산 요청 내역이 정상 승인되었습니다.
(요청일/정산유형/상세/금액)

{dm_content}
"""
                            send_dm(user_id, server_nick, msg)

                        st.session_state["msg1"] = ("success", "승인이 완료되었습니다.")
                        custom_rerun()
                    else:
                        st.session_state["msg1"] = ("warning", "승인할 데이터가 없습니다.")
        with col2:
            reject_btn1 = st.form_submit_button("반려", key="reject_btn1", width="stretch")
            if reject_btn1:
                with st.spinner(f"In progress...", show_time=True):
                    edit_df["select_yn"] = edit_df["select_yn"].apply(lambda x: "y" if x else "n")
                    selected_df = edit_df[edit_df["select_yn"] == "y"].reset_index(drop=True)
                    if len(selected_df) >= 1:
                        for _, row in selected_df.iterrows():
                            idx = row["idx"]
                            cond1 = row["charge_type"] == "벌금"
                            cond2 = "미투표" in row["charge_detail"]
                            if cond1 & cond2:
                                # deposit_date 셀값 삭제
                                update_cell("tbl_charge_inf_his", f"F{idx+2}", "")
                                # user_check_yn 셀값 변경: y->n
                                update_cell("tbl_charge_inf_his", f"H{idx+2}", "n")
                            else:
                                # valid_yn 셀값 변경: y->n
                                update_cell("tbl_charge_inf_his", f"J{idx+2}", "n")

                        # DM 발송
                        user_info_df = selected_df[["user_id", "server_nick"]].drop_duplicates().reset_index(drop=True)
                        for idx, row in user_info_df.iterrows():
                            user_id = row["user_id"]
                            server_nick = row["server_nick"]
                            user_charge_df = selected_df[selected_df["user_id"]==user_id][["request_date", "charge_type", "charge_detail", "amount"]].reset_index(drop=True)
                            user_charge_df["dm_content"] = user_charge_df.apply(lambda x: f"- {x['request_date']} / {x['charge_type']} / {x['charge_detail']} / {x['amount']}", axis=1)
                            dm_content_lst = user_charge_df["dm_content"].tolist()
                            dm_content = "\n".join(dm_content_lst)
                            msg = f"""## [DM] 정산 요청 결과 알림(반려)
🎈안녕하세요. {server_nick}님.
아래 정산 요청 내역이 반려되었습니다.
(요청일/정산유형/상세/금액)

{dm_content}
"""
                            send_dm(user_id, server_nick, msg)

                        st.session_state["msg1"] = ("success", "반려가 완료되었습니다.")
                        custom_rerun()
                    else:
                        st.session_state["msg1"] = ("warning", "반려할 데이터가 없습니다.")
        show_msg("msg1")

    st.markdown("##### 2. 휴면 요청 현황")
    approval_btn2, reject_btn2 = False, False
    with st.form(key="req_approval_form2"):
        cond1 = (dormant_df["dormant_yn"]=="y") & (dormant_df["dormant_admin_yn"]=="n")
        cond2 = (dormant_df["withdrawal_yn"]=="y") & (dormant_df["withdrawal_admin_yn"]=="n")
        cond3 = dormant_df["valid_yn"]=="y"
        dormant_df = dormant_df[(cond1|cond2)&cond3].reset_index(drop=True)

        dormant_df["select_yn"] = False
        dormant_df["withdrawal_yn"] = dormant_df["withdrawal_yn"].map({"y": "철회", "n": "신청"}).fillna(dormant_df["withdrawal_yn"])
        edit_df = st.data_editor(
            dormant_df,
            column_config={
                "select_yn": st.column_config.CheckboxColumn("선택", disabled=False),
                "server_nick": st.column_config.TextColumn("이름", disabled=True),
                "yearmonth": st.column_config.TextColumn("대상 기간", disabled=True),
                "withdrawal_yn": st.column_config.TextColumn("구분", disabled=True),
            },
            column_order=["select_yn", "withdrawal_yn", "server_nick", "yearmonth"],
            num_rows="fixed",
            hide_index=True,
            width="stretch",
            key="admin_approval_dormant_editor"
        )
        col1, col2 = st.columns([1, 1])
        with col1:
            approval_btn2 = st.form_submit_button("승인", key="approval_btn2", use_container_width=True)
            if approval_btn2:
                with st.spinner(f"In progress...", show_time=True):
                    edit_df["select_yn"] = edit_df["select_yn"].apply(lambda x: "y" if x else "n")
                    selected_df = edit_df[edit_df["select_yn"]=="y"].reset_index(drop=True)
                    if len(selected_df) >= 1:
                        for _, row in selected_df.iterrows():
                            idx = row["idx"]
                            # withdrawal_admin_yn: n>y
                            if row.withdrawal_yn=="철회":
                                update_cell("tbl_mbr_dormant_his", f"H{idx+2}", "y")
                                # 요청기간이 현시점이면 active_yn:>y
                                if row.yearmonth==today_yyyymm:
                                    user_idx = mbr_df[mbr_df.user_id==user_id]["idx"].values[0]
                                    update_cell("tbl_mbr_inf_snp", f"N{user_idx+2}", "y")
                            # dormant_admin_yn: n>y
                            elif row.withdrawal_yn=="신청":
                                update_cell("tbl_mbr_dormant_his", f"F{idx+2}", "y")
                                # 요청기간이 현시점이면 active_yn:>n
                                if row.yearmonth==today_yyyymm:
                                    user_idx = mbr_df[mbr_df.user_id==user_id]["idx"].values[0]
                                    update_cell("tbl_mbr_inf_snp", f"N{user_idx+2}", "n")

                        tmp_df = selected_df[["user_id", "yearmonth", "withdrawal_yn"]].reset_index(drop=True)
                        tmp_df['month_cnt'] = tmp_df['withdrawal_yn'].map({'신청': 1, '철회': -1})
                        month_cnt_df = tmp_df.groupby('user_id', as_index=False)['month_cnt'].sum()
                        user_ids = month_cnt_df["user_id"].tolist()

                        # due_date 업데이트
                        for m_idx, m_row in mbr_df.iterrows():
                            if m_row["user_id"] in user_ids:
                                due_date = str(m_row["due_date"]).replace("'", "")
                                add_month = int(month_cnt_df[month_cnt_df["user_id"] == m_row["user_id"]]["month_cnt"].values[0])
                                new_due_date_obj = datetime.strptime(due_date, "%Y%m") + relativedelta(months=add_month)
                                new_due_date = new_due_date_obj.strftime("%Y%m")
                                update_cell("tbl_mbr_inf_snp", f"M{m_idx+2}", "'" + new_due_date)

                        # DM 발송
                        user_info_df = selected_df[["user_id", "server_nick"]].drop_duplicates().reset_index(drop=True)
                        for idx, row in user_info_df.iterrows():
                            user_id = row["user_id"]
                            server_nick = row["server_nick"]
                            user_charge_df = selected_df[selected_df["user_id"]==user_id][["request_date", "yearmonth", "withdrawal_yn"]].reset_index(drop=True)
                            user_charge_df["withdrawal_yn"] = user_charge_df["withdrawal_yn"].map({"y": "철회", "n": "신청"}).fillna(user_charge_df["withdrawal_yn"])
                            user_charge_df["dm_content"] = user_charge_df.apply(lambda x: f"- {x['request_date']} / {x['yearmonth']} / {x['withdrawal_yn']}", axis=1)
                            dm_content_lst = user_charge_df["dm_content"].tolist()
                            dm_content = "\n".join(dm_content_lst)
                            msg = f"""## [DM] 휴면 요청 결과 알림(승인)
🎈안녕하세요. {server_nick}님.
아래 휴면 요청 내역이 정상 승인되었습니다.
(요청일/기간/구분)

{dm_content}
"""
                            send_dm(user_id, server_nick, msg)

                        st.session_state['msg2'] = ("success", "승인이 완료되었습니다.")
                        custom_rerun()
                    else:
                        st.session_state['msg2'] = ("warning", "승인할 데이터가 없습니다.")
        with col2:
            reject_btn2 = st.form_submit_button("반려", key="reject_btn2", use_container_width=True)
            if reject_btn2:
                with st.spinner(f"In progress...", show_time=True):
                    edit_df["select_yn"] = edit_df["select_yn"].apply(lambda x: "y" if x else "n")
                    selected_df = edit_df[edit_df["select_yn"]=="y"].reset_index(drop=True)
                    if len(selected_df) >= 1:
                        # valid_yn 셀값 변경: n->y
                        for _, row in selected_df.iterrows():
                            idx = row["idx"]
                            # valid_yn:y>n
                            update_cell("tbl_mbr_dormant_his", f"I{idx+2}", "n")
                            # withdrawal_admin_yn: n>y
                            if row.withdrawal_yn=="철회":
                                update_cell("tbl_mbr_dormant_his", f"H{idx+2}", "y")
                            # dormant_admin_yn: n>y
                            elif row.withdrawal_yn=="신청":
                                update_cell("tbl_mbr_dormant_his", f"F{idx+2}", "y")

                        # DM 발송
                        user_info_df = selected_df[["user_id", "server_nick"]].drop_duplicates().reset_index(drop=True)
                        for idx, row in user_info_df.iterrows():
                            user_id = row["user_id"]
                            server_nick = row["server_nick"]
                            user_charge_df = selected_df[selected_df["user_id"]==user_id][["request_date", "yearmonth", "withdrawal_yn"]].reset_index(drop=True)
                            user_charge_df["withdrawal_yn"] = user_charge_df["withdrawal_yn"].map({"y": "철회", "n": "신청"}).fillna(user_charge_df["withdrawal_yn"])
                            user_charge_df["dm_content"] = user_charge_df.apply(lambda x: f"- {x['request_date']} / {x['yearmonth']} / {x['withdrawal_yn']}", axis=1)
                            dm_content_lst = user_charge_df["dm_content"].tolist()
                            dm_content = "\n".join(dm_content_lst)
                            msg = f"""## [DM] 휴면 요청 결과 알림(반려)
🎈안녕하세요. {server_nick}님.
아래 휴면 요청 내역이 반려되었습니다.
(요청일/기간/구분)

{dm_content}
"""
                            send_dm(user_id, server_nick, msg)

                        st.session_state['msg2'] = ("success", "반려가 완료되었습니다.")
                        custom_rerun()
                    else:
                        st.session_state['msg2'] = ("warning", "반려할 데이터가 없습니다.")
        show_msg("msg2")

def page_main():
    """메인 페이지"""
    # 페이지 설정
    st.set_page_config(
        page_title=WEBAPP_NAME,
        page_icon="⚽️",
        layout="centered",
        initial_sidebar_state="collapsed"
    )
    # 세션에서 유저 정보 불러오기 (가정: 서버닉네임/권한이 세션에 저장됨)
    user_id = st.session_state.get("user_id", "")
    user_nick = st.session_state.get("server_nick", "Unknown")
    admin_yn = st.session_state.get("admin_yn", "n")  # "Y" or "N"

    # 권한 텍스트 및 배지 색상 결정
    if admin_yn == "y":
        role_label = "Admin"
        badge_color = "#3DDAD7" # mint
    else:
        role_label = "Everyone"
        badge_color = "#888888" # gray

    # 상단 닉네임 + 권한 표시
    st.markdown(
        f"""
        <div style='display: flex; align-items: center; margin-bottom: 0.5rem;'>
            <span style='font-size: 1.5rem; font-weight: 600; margin-right: 0.75rem;'>{user_nick}</span>
            <span style="
                background: {badge_color}; 
                color: white; 
                border-radius: 1rem; 
                padding: 0.2rem 0.8rem;
                font-size: 1rem;
                font-weight: 500;
                display: inline-block;
            ">
                {role_label}
            </span>
        </div>
        """, unsafe_allow_html=True
    )

    # 메뉴
    st.markdown("---")
    st.markdown("##### 메뉴 선택")
    menu_items = ["대시보드", "정산요청", "휴면요청","요청현황"]
    admin_menu_items = ["승인(Admin)"]
    if admin_yn == "y":
        menu_items += admin_menu_items

    selected_menu = st.selectbox(
        label="메인 메뉴",
        options=menu_items,
        key="main_menu_select"
    )   

    # 메뉴 선택
    if selected_menu==menu_items[0]:
        menu_dashboard()
    elif selected_menu==menu_items[1]:
        menu_charge_req()
    elif selected_menu==menu_items[2]:
        menu_dormant_request()
    elif selected_menu==menu_items[3]:
        menu_request_status()
    elif selected_menu==admin_menu_items[0]:
        menu_admin_approval()

    # 하단 기본 정보
    st.divider()
    st.caption(f"© {today_yyyy} {WEBAPP_NAME}. All rights reserved.")

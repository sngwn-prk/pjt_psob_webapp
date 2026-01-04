# Python Standard Packages
import streamlit as st
import re
import time
import os
import json
import time
import requests
import random
import string
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
# from dotenv import load_dotenv
# load_dotenv()
from tenacity import retry, stop_after_attempt, wait_exponential

# Google Spreadsheet API
# from oauth2client.service_account import ServiceAccountCredentials
from streamlit_gsheets import GSheetsConnection
import gspread
from google.oauth2.service_account import Credentials

WEBAPP_NAME = "PowerSupplyOB"
# SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
# DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
# DISCORD_BOT_ID = os.getenv("DISCORD_BOT_ID")
DISCORD_BOT_TOKEN = st.secrets["DISCORD_BOT_TOKEN"]
DISCORD_BOT_ID = st.secrets["DISCORD_BOT_ID"]
YEAR = datetime.now().strftime("%Y")

def format_phone_number(phone):
    try:
        if isinstance(phone, float):
            phone = int(phone)
        
        phone_str = str(phone).replace('.0', '')
        
        if len(phone_str) == 10 and phone_str.isdigit():
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
    spreadsheet_url = connection_info["spreadsheet"]
    spreadsheet = gc.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    return worksheet

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def read_sheet(sheetname:str):
    """
    지정된 시트의 데이터를 읽어옵니다.
    """
    try:
        conn = st.connection(sheetname, type=GSheetsConnection, ttl=0)
        df = conn.read(worksheet=sheetname, ttl=0)
        if 'phn_no' in df.columns:
            df['phn_no'] = df['phn_no'].apply(format_phone_number)
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(str)
        return df
    except Exception as e:
        print(e)
        return None

def generate_verification_code():
    """6자리 인증번호 생성"""
    return ''.join(random.choices(string.digits, k=6))

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.01, min=0.05, max=0.1))
def send_dm(user_id:str, server_nick:str, msg:str):
    """
    특정 유저에게 DM을 전송하는 함수입니다.
    """
    try:
        st.write(f"{user_id} / {server_nick}")
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
        if response.status_code in [200, 201]:
            return True
        else:
            # 메시지 기록 방식 변경
            st.session_state.verification_message = {"type": "error", "text": "❌ DM 발송에 실패했습니다."}
            return False
    except Exception as e:
        st.session_state.verification_message = {
            "type": "error",
            "text": f"⚠️ DM 발송 중 오류 발생. 관리자에게 문의하세요. (오류 내용: {e})"
        }
        return False

def show_verification_message():
    vm = st.session_state.get("verification_message", None)
    if vm is not None:
        if vm.get("type") == "success":
            st.success(vm.get("text", ""))
        elif vm.get("type") == "error":
            st.error(vm.get("text", ""))
        elif vm.get("type") == "info":
            st.info(vm.get("text", ""))
        elif vm.get("type") == "warning":
            st.warning(vm.get("text", ""))

def page_login():
    """핸드폰 번호 입력 및 인증번호 확인 페이지"""

    # 페이지 설정
    st.set_page_config(
        page_title=WEBAPP_NAME,
        page_icon="⚽️",
        layout="centered",
        initial_sidebar_state="collapsed"
    )
    # 헤더
    st.title(f"{WEBAPP_NAME}")
    st.subheader("Login")

    # 회원 정보
    df = read_sheet("tbl_mbr_inf_snp")
    df = df[["user_id", "server_nick", "phn_no", "admin_yn"]]
    
    # 현재 단계 확인
    current_step = st.session_state.get("step", "phone_input")
    
    # 인증번호 입력 단계
    if current_step == "verification":
        # 타임아웃 체크 (60초)
        if "cert_code_sent_time" in st.session_state:
            elapsed_time = time.time() - st.session_state.cert_code_sent_time
            if elapsed_time > 60:
                st.session_state.verification_message = {
                    "type": "error",
                    "text": "❌ 제한시간 내에 인증번호를 입력하세요."
                }
                st.session_state.step = "phone_input"
                st.session_state.cert_code = None
                st.session_state.cert_code_sent_time = None
                st.rerun()
        
        # 인증번호 입력 UI
        input_code = st.text_input(
            "Discord DM으로 발송된 인증번호를 입력하세요. (제한시간: 1분)",
            placeholder="6자리 숫자"
        )
        
        # 메시지 위치: 버튼 하단, 항상 같은 위치를 위한 컨테이너
        message_placeholder = st.empty()

        col1, col2 = st.columns([1, 1])

        # 인증번호 확인 처리
        with col1:
            if st.button("인증번호 확인", width="stretch"):
                if not input_code or len(input_code) != 6 or not input_code.isdigit():
                    st.session_state.verification_message = {
                        "type": "error",
                        "text": "❌ 6자리 숫자를 정확히 입력하세요."
                    }
                elif input_code != st.session_state.get("cert_code", ""):
                    st.session_state.verification_message = {
                        "type": "error",
                        "text": "❌ 인증번호가 일치하지 않습니다."
                    }
                else:
                    # 인증 성공 - admin_yn에 따라 모드 설정
                    admin_yn = st.session_state.get("admin_yn", "n")
                    if admin_yn.lower() == "y":
                        st.session_state.user_mode = "admin"
                    else:
                        st.session_state.user_mode = "everyone"
                    
                    st.session_state.logged_in = True
                    st.session_state.phone_number = st.session_state.get("phn_no", "")
                    st.session_state.step = "main"
                    time.sleep(0.5)
                    st.rerun()
        
        # 인증번호 재발송 처리
        with col2:
            if st.button("재발송", width="stretch"):
                phn_no = st.session_state.get("phn_no", "")
                mbr_df = df[df["phn_no"] == phn_no].copy()
                
                if len(mbr_df) > 0:
                    mbr_dict = mbr_df.iloc[0].to_dict()
                    user_id = mbr_dict["user_id"]
                    server_nick = mbr_dict["server_nick"]
                    admin_yn = mbr_dict["admin_yn"]
                    
                    # 새 인증번호 생성 및 세션 상태 저장
                    cert_code = generate_verification_code()
                    st.session_state.cert_code = cert_code
                    st.session_state.cert_code_sent_time = time.time()
                    st.session_state.admin_yn = admin_yn
                    
                    # DM 재발송
                    msg = f"## [DM] WebApp 인증번호\n__**{cert_code}**__"
                    send_dm(user_id, server_nick, msg)
                    st.session_state.verification_message = {
                        "type": "success",
                        "text": "✅ 인증번호가 재발송되었습니다."
                    }
                    st.rerun()
                else:
                    st.session_state.verification_message = {
                        "type": "error",
                        "text": "❌ 등록되지 않은 회원입니다. 운영진에게 문의하세요."
                    }
        
        # 메시지(오류/안내) 표시: 버튼 바로 아래
        vm = st.session_state.get("verification_message", None)
        if vm is not None:
            with message_placeholder:
                if vm.get("type") == "success":
                    st.success(vm.get("text", ""))
                elif vm.get("type") == "error":
                    st.error(vm.get("text", ""))
                elif vm.get("type") == "info":
                    st.info(vm.get("text", ""))
                elif vm.get("type") == "warning":
                    st.warning(vm.get("text", ""))
            # 메시지 초기화
            del st.session_state.verification_message

    # 연락처 입력 단계
    else:
        # 메인 컨테이너
        with st.container():
            # 연락처 입력
            phn_no = st.text_input(
                "숫자만 입력하세요. ('-' 제외)",
                value=st.session_state.get("phone_number", ""),
                placeholder="01012345678",
                help="하이픈(-) 없이 숫자만 입력하세요. 관리자를 통해 사전에 등록된 사용자만 접근 가능합니다."
            )
            
            # 인증번호 발송 버튼 아래에 메시지 표시 (항상 같은 자리)
            message_placeholder = st.empty()

            # 인증번호 발송 버튼이 눌린 경우
            if st.button("인증번호 발송", width="stretch"):
                cond1 = len(phn_no) != 11
                cond2 = not phn_no.isdigit()
                mbr_df = df[df["phn_no"] == phn_no].copy()
                cond3 = len(mbr_df) == 0
                if cond1 or cond2:
                    st.session_state.verification_message = {
                        "type": "error",
                        "text": "❌ 연락처는 11자리 숫자만 입력하세요."
                    }
                elif cond3:
                    st.session_state.verification_message = {
                        "type": "error",
                        "text": "❌ 등록되지 않은 회원입니다. 운영진에게 문의하세요."
                    }
                else:
                    mbr_dict = mbr_df.iloc[0].to_dict()
                    user_id = mbr_dict["user_id"]
                    server_nick = mbr_dict["server_nick"]
                    admin_yn = mbr_dict["admin_yn"]

                    # 인증번호 생성 및 세션 상태 저장
                    cert_code = generate_verification_code()
                    st.session_state.cert_code = cert_code
                    st.session_state.cert_code_sent_time = time.time()
                    st.session_state.user_id = user_id
                    st.session_state.server_nick = server_nick
                    st.session_state.phn_no = phn_no
                    st.session_state.admin_yn = admin_yn

                    # DM 발송
                    msg = f"## [DM] WebApp 인증번호\n__**{cert_code}**__"
                    send_dm(user_id, server_nick, msg)
                    st.session_state.step = "verification"
                    time.sleep(0.1)
                    st.rerun()

            # 메시지(오류/안내) 표시: 버튼 바로 아래
            vm = st.session_state.get("verification_message", None)
            if vm is not None:
                with message_placeholder:
                    if vm.get("type") == "success":
                        st.success(vm.get("text", ""))
                    elif vm.get("type") == "error":
                        st.error(vm.get("text", ""))
                    elif vm.get("type") == "info":
                        st.info(vm.get("text", ""))
                    elif vm.get("type") == "warning":
                        st.warning(vm.get("text", ""))
                # 메시지 초기화
                del st.session_state.verification_message

    # 하단 정보
    st.divider()
    st.caption(f"© {YEAR} {WEBAPP_NAME}. All rights reserved.")

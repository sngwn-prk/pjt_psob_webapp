import os

import streamlit as st
from pages.page_login import page_login
from pages.page_main import page_main

## 스트림릿 환경변수
# DEVELOPER_EMAIL = st.secrets["DEVELOPER_EMAIL"]

## 로컬 환경변수
from dotenv import load_dotenv
load_dotenv()
DEVELOPER_EMAIL = os.getenv("DEVELOPER_EMAIL")

# # Streamlit 메시지 비활성화
# os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
# os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"

WEBAPP_NAME = "PowerSupplyOB"

def main():
    # 세션 상태 초기화
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "step" not in st.session_state:
        st.session_state.step = "phone_input"  # phone_input, verification, main
    
    # 로그인 상태에 따라 페이지 표시
    if st.session_state.logged_in:
        page_main()
    else:
        page_login()

if __name__ == "__main__":
    main()
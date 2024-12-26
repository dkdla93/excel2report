import streamlit as st
import pandas as pd
from jinja2 import Template
from datetime import datetime
import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import traceback
from io import BytesIO
import zipfile

# 페이지 기본 설정
st.set_page_config(
    page_title="크리에이터 보고서 생성기",
    page_icon="📊",
    layout="wide"
)

class CreatorInfoHandler:
    def __init__(self, info_file):
        """크리에이터 정보 파일을 읽어서 초기화합니다."""
        self.creator_info = pd.read_excel(info_file)
        self.creator_info.set_index('아이디', inplace=True)
    
    def get_commission_rate(self, creator_id):
        """크리에이터의 수수료율을 반환합니다."""
        return self.creator_info.loc[creator_id, 'percent']
    
    def get_email(self, creator_id):
        """크리에이터의 이메일 주소를 반환합니다."""
        return self.creator_info.loc[creator_id, 'email']
    
    def get_all_creator_ids(self):
        """모든 크리에이터 ID를 반환합니다."""
        return list(self.creator_info.index)

class GmailAPI:
    def __init__(self, credentials_file):
        """Gmail API 초기화"""
        self.SCOPES = ['https://www.googleapis.com/auth/gmail.send']
        self.creds = None
        self.credentials_file = credentials_file

    def authenticate(self):
        """Gmail API 인증을 수행합니다."""
        if 'gmail_token' in st.session_state:
            self.creds = pickle.loads(st.session_state['gmail_token'])

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES)
                self.creds = flow.run_local_server(port=0)

            # 세션 상태에 토큰 저장
            st.session_state['gmail_token'] = pickle.dumps(self.creds)

        return build('gmail', 'v1', credentials=self.creds)

    def send_report(self, to_email, creator_name, report_content):
        """보고서를 첨부하여 이메일을 발송합니다."""
        try:
            service = self.authenticate()

            message = MIMEMultipart()
            message['to'] = to_email
            message['subject'] = f"{creator_name} 크리에이터님의 음원 사용현황 보고서"

            body = f"""안녕하세요, {creator_name} 크리에이터님

첨부된 파일을 통해 음원 사용현황을 확인해주세요.
문의사항이 있으시면 언제든 연락 주시기 바랍니다.

감사합니다."""

            message.attach(MIMEText(body, 'plain'))

            # 보고서 첨부
            report = MIMEApplication(report_content, _subtype='html')
            report.add_header('Content-Disposition', 'attachment', 
                            filename=f'{creator_name}_report.html')
            message.attach(report)

            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
            service.users().messages().send(
                userId='me',
                body={'raw': raw_message}
            ).execute()

            st.success(f"{creator_name} 크리에이터에게 이메일 발송 완료")
            return True

        except Exception as e:
            st.error(f"이메일 발송 실패 ({creator_name}): {str(e)}")
            return False

def clean_numeric_value(value):
    """숫자 값을 안전하게 정수로 변환합니다."""
    try:
        if pd.isna(value):
            return 0
        if isinstance(value, str):
            value = value.replace(',', '')
        return int(float(value))
    except (ValueError, TypeError):
        return 0

def create_video_data(df):
    """데이터프레임에서 비디오 데이터를 추출합니다."""
    video_data = []
    for _, row in df.iterrows():
        if pd.isna(row['동영상 제목']):  # 제목이 없는 행은 건너뛰기
            continue
            
        video_data.append({
            'title': str(row['동영상 제목']),
            'views': clean_numeric_value(row['조회수']),
            'revenueBefore': clean_numeric_value(row['대략적인 파트너 수익 (KRW)']),
            'revenueAfter': clean_numeric_value(row['수수료 제외 후 수익'])
        })
    return video_data

def process_data(input_df, creator_info_handler, gmail_api=None, send_email=False):
    """데이터를 처리하고 보고서를 생성합니다."""
    reports_data = {}
    excel_files = {}
    
    try:
        # 크리에이터별 처리
        for creator_id in creator_info_handler.get_all_creator_ids():
            try:
                st.write(f"\n{creator_id} 크리에이터 처리 시작...")
                
                # 해당 크리에이터의 데이터만 필터링
                creator_data = input_df[input_df['아이디'] == creator_id].copy()
                if creator_data.empty:
                    st.warning(f"{creator_id} 크리에이터의 데이터가 없습니다.")
                    continue
                
                # NaN 값 처리
                creator_data['조회수'] = creator_data['조회수'].fillna(0)
                creator_data['대략적인 파트너 수익 (KRW)'] = creator_data['대략적인 파트너 수익 (KRW)'].fillna(0)
                
                # 수수료율 적용
                commission_rate = creator_info_handler.get_commission_rate(creator_id)
                
                # 데이터 처리
                total_views = clean_numeric_value(creator_data['조회수'].sum())
                total_revenue_before = clean_numeric_value(creator_data['대략적인 파트너 수익 (KRW)'].sum())
                total_revenue_after = int(total_revenue_before * commission_rate)
                
                # 조회수 기준 상위 50개 필터링
                filtered_data = creator_data.nlargest(50, '조회수').copy()
                filtered_data['수수료 제외 후 수익'] = filtered_data['대략적인 파트너 수익 (KRW)'] * commission_rate
                
                # 총계 행 추가
                total_row = pd.Series({
                    '동영상 제목': '총계',
                    '조회수': total_views,
                    '대략적인 파트너 수익 (KRW)': total_revenue_before,
                    '수수료 제외 후 수익': total_revenue_after
                }, name='total')
                
                filtered_data = pd.concat([filtered_data, pd.DataFrame([total_row])], ignore_index=True)
                
                # 엑셀 파일 생성
                excel_buffer = BytesIO()
                filtered_data.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                excel_files[f"{creator_id}.xlsx"] = excel_buffer.getvalue()
                
                # 보고서 데이터 생성
                report_data = {
                    'creatorName': creator_id,
                    'period': datetime.now().strftime("%y.%m.01 - %y.%m.30"),
                    'totalRevenue': total_revenue_after,
                    'totalViews': total_views,
                    'videoData': create_video_data(filtered_data[:-1])  # 총계 행 제외
                }
                
                # HTML 생성
                html_content = generate_html_report(report_data)
                reports_data[f"{creator_id}_report.html"] = html_content
                
                # 이메일 발송
                if send_email and gmail_api:
                    email = creator_info_handler.get_email(creator_id)
                    if email:
                        gmail_api.send_report(email, creator_id, html_content.encode('utf-8'))
                
                st.success(f"{creator_id} 크리에이터 처리 완료")
                
            except Exception as e:
                st.error(f"{creator_id} 크리에이터 처리 중 오류 발생: {str(e)}")
                st.write(traceback.format_exc())
                continue
                
    except Exception as e:
        st.error(f"전체 처리 중 오류 발생: {str(e)}")
        st.write(traceback.format_exc())
        
    return reports_data, excel_files

def generate_html_report(data):
    """HTML 보고서를 생성합니다."""
    try:
        # 템플릿 파일 읽기
        template_path = 'templates/template.html'
        with open(template_path, 'r', encoding='utf-8') as f:
            template_str = f.read()
        
        template = Template(template_str)
        template.globals['format_number'] = lambda x: "{:,}".format(int(x))
        
        return template.render(**data)
        
    except Exception as e:
        st.error(f"HTML 생성 실패 ({data['creatorName']}): {str(e)}")
        st.write(traceback.format_exc())
        return None

def create_zip_file(reports_data, excel_files):
    """보고서와 엑셀 파일들을 ZIP 파일로 압축합니다."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # HTML 보고서 추가
        for filename, content in reports_data.items():
            zip_file.writestr(f"reports/{filename}", content)
        
        # 엑셀 파일 추가
        for filename, content in excel_files.items():
            zip_file.writestr(f"excel/{filename}", content)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def main():
    st.title("크리에이터 보고서 생성기")
    
    # 파일 업로드
    st.header("데이터 파일 업로드")
    creator_info = st.file_uploader("크리에이터 정보 파일 (creator_info.xlsx)", type=['xlsx'])
    statistics = st.file_uploader("통계 데이터 파일 (creator_statistics.xlsx)", type=['xlsx'])
    
    # 이메일 발송 옵션
    st.header("이메일 발송 설정")
    send_email = st.checkbox("보고서를 이메일로 발송하기")
    credentials_file = None
    if send_email:
        credentials_file = st.file_uploader("Gmail 인증 파일 (credentials.json)", type=['json'])
    
    if st.button("보고서 생성"):
        if creator_info and statistics:
            try:
                # 데이터 로드
                creator_info_handler = CreatorInfoHandler(creator_info)
                statistics_df = pd.read_excel(statistics)
                
                # Gmail API 초기화 (이메일 발송 선택 시)
                gmail_api = None
                if send_email and credentials_file:
                    temp_cred_path = "temp_credentials.json"
                    with open(temp_cred_path, 'wb') as f:
                        f.write(credentials_file.getvalue())
                    gmail_api = GmailAPI(temp_cred_path)
                
                # 처리 상태 표시
                with st.spinner('보고서 생성 중...'):
                    # 데이터 처리 및 보고서 생성
                    reports_data, excel_files = process_data(
                        statistics_df, 
                        creator_info_handler,
                        gmail_api,
                        send_email
                    )
                    
                    # ZIP 파일 생성
                    zip_data = create_zip_file(reports_data, excel_files)
                    
                    # 다운로드 버튼
                    st.download_button(
                        label="보고서 다운로드",
                        data=zip_data,
                        file_name="reports.zip",
                        mime="application/zip"
                    )
                
                # 임시 파일 정리
                if send_email and credentials_file:
                    os.remove(temp_cred_path)
                
            except Exception as e:
                st.error(f"처리 중 오류가 발생했습니다: {str(e)}")
                st.write(traceback.format_exc())
        else:
            st.warning("필요한 파일을 모두 업로드해주세요.")

if __name__ == "__main__":
    main()
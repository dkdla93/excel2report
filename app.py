import streamlit as st
import pandas as pd
from jinja2 import Template
from datetime import datetime
import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import traceback
from io import BytesIO
import zipfile
import numpy as np
from weasyprint import HTML, CSS
import requests
import smtplib

# 페이지 기본 설정
st.set_page_config(
    page_title="크리에이터 보고서 생성기",
    page_icon="📊",
    layout="wide"
)

class DataValidator:
    def __init__(self, original_df, creator_info_handler):
        """데이터 검증을 위한 초기화"""
        self.original_df = original_df
        self.summary_row = original_df.iloc[0]  # 2행(인덱스 0)의 합계 데이터
        self.data_rows = original_df.iloc[1:]   # 3행(인덱스 1)부터의 실제 데이터
        self.creator_info_handler = creator_info_handler
        self.commission_rates = self._get_commission_rates()
        self.total_stats = self._calculate_total_stats()
        self.creator_stats = self._calculate_creator_stats()

    def _get_commission_rates(self):
        """크리에이터별 수수료율을 가져옵니다."""
        return {creator_id: self.creator_info_handler.get_commission_rate(creator_id) 
                for creator_id in self.creator_info_handler.get_all_creator_ids()}

    def _calculate_total_stats(self):
        """전체 통계를 계산합니다."""
        creator_revenues = self.data_rows.groupby('아이디').agg({
            '대략적인 파트너 수익 (KRW)': 'sum'
        })
        total_revenue_after = sum(
            revenue * self.commission_rates.get(creator_id, 0)
            for creator_id, revenue in creator_revenues['대략적인 파트너 수익 (KRW)'].items()
        )
        
        summary_stats = {
            'creator_count': len(self.data_rows['아이디'].unique()),
            'total_views_summary': self.summary_row['조회수'],
            'total_revenue_summary': self.summary_row['대략적인 파트너 수익 (KRW)'],
            'total_views_data': self.data_rows['조회수'].sum(),
            'total_revenue_data': self.data_rows['대략적인 파트너 수익 (KRW)'].sum(),
            'total_revenue_after': total_revenue_after
        }
        return summary_stats

    def _calculate_creator_stats(self):
        """크리에이터별 통계를 계산합니다."""
        grouped = self.data_rows.groupby('아이디').agg({
            '조회수': 'sum',
            '대략적인 파트너 수익 (KRW)': 'sum'
        }).reset_index()
        return grouped

    def compare_creator_stats(self, processed_df):
        """크리에이터별 통계를 비교합니다."""
        processed_creator_stats = self._calculate_creator_stats()
        merged_stats = pd.merge(
            self.creator_stats,
            processed_creator_stats,
            on='아이디',
            suffixes=('_original', '_processed')
        )
        merged_stats['views_match'] = abs(merged_stats['조회수_original'] - merged_stats['조회수_processed']) < 1
        merged_stats['revenue_match'] = abs(
            merged_stats['대략적인 파트너 수익 (KRW)_original'] -
            merged_stats['대략적인 파트너 수익 (KRW)_processed']
        ) < 1
        return merged_stats

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


def show_validation_results(original_df, processed_df, creator_info_handler):
    """검증 결과를 표시합니다."""
    st.header("🔍 처리 결과 검증")
    
    validator = DataValidator(original_df, creator_info_handler)
    
    # 전체 데이터 요약 - 표 형식 변경
    st.subheader("전체 데이터 요약")
    summary_data = {
        '전체 크리에이터 수': validator.total_stats['creator_count'],
        '총 조회수': validator.total_stats['total_views_data'],
        '총 수익': validator.total_stats['total_revenue_data'],
        '정산 후 총 수익': validator.total_stats['total_revenue_after']
    }
    summary_df = pd.DataFrame([summary_data])
    st.dataframe(summary_df.style.format({
        '총 조회수': '{:,}',
        '총 수익': '₩{:,}',
        '정산 후 총 수익': '₩{:,}'
    }), use_container_width=True)
    
    # 전체 데이터 검증
    st.subheader("전체 데이터 검증")
    comparison_df = pd.DataFrame({
        '원본 데이터': [
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data']
        ],
        '처리 후 데이터': [
            processed_df['조회수'].sum(),
            processed_df['대략적인 파트너 수익 (KRW)'].sum()
        ],
        '일치 여부': [
            abs(validator.total_stats['total_views_data'] - processed_df['조회수'].sum()) < 1,
            abs(validator.total_stats['total_revenue_data'] - processed_df['대략적인 파트너 수익 (KRW)'].sum()) < 1
        ]
    }, index=['총 조회수', '총 수익'])
    
    st.dataframe(
        comparison_df.style.format({
            '원본 데이터': '{:,.0f}',
            '처리 후 데이터': '{:,.0f}'
        }).apply(
            lambda x: ['background-color: #e6ffe6' if v else 'background-color: #ffe6e6' for v in x], 
            subset=['일치 여부']
        ),
        use_container_width=True
    )

    # 크리에이터별 검증
    st.subheader("크리에이터별 검증")
    creator_comparison = validator.compare_creator_stats(processed_df)
    creator_comparison['수수료율'] = creator_comparison['아이디'].map(
        lambda x: creator_info_handler.get_commission_rate(x)
    )
    creator_comparison['수수료 후 수익'] = creator_comparison['대략적인 파트너 수익 (KRW)_processed'] * creator_comparison['수수료율']
    
    # 칼럼 순서 재정렬
    columns_order = [
        '아이디',
        '조회수_original',
        '조회수_processed',
        'views_match',
        '대략적인 파트너 수익 (KRW)_original',
        '대략적인 파트너 수익 (KRW)_processed',
        'revenue_match',
        '수수료율',
        '수수료 후 수익'
    ]
    
    creator_comparison = creator_comparison[columns_order]
    
    st.dataframe(
        creator_comparison.style.format({
            '조회수_original': '{:,.0f}',
            '조회수_processed': '{:,.0f}',
            '대략적인 파트너 수익 (KRW)_original': '₩{:,.0f}',
            '대략적인 파트너 수익 (KRW)_processed': '₩{:,.0f}',
            '수수료율': '{:.2%}',
            '수수료 후 수익': '₩{:,.0f}'
        }).apply(
            lambda x: ['background-color: #e6ffe6' if v else 'background-color: #ffe6e6' for v in x], 
            subset=['views_match', 'revenue_match']
        ),
        use_container_width=True
    )



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

def generate_html_report(data):
    """HTML 보고서를 생성합니다."""
    try:
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

def create_pdf_from_html(html_content, creator_id):
    """HTML 내용을 PDF로 변환합니다."""
    try:
        landscape_css = CSS(string="""
            @font-face {
                font-family: 'NanumGothic';
                src: local('NanumGothic');
            }
            @page {
                size: A4 landscape;
                margin: 10mm;
            }
            body {
                font-family: 'NanumGothic', 'Noto Sans CJK KR', sans-serif;
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
        """)
        
        pdf_buffer = BytesIO()
        HTML(string=html_content).write_pdf(pdf_buffer, stylesheets=[landscape_css])
        pdf_buffer.seek(0)
        return pdf_buffer.getvalue()
        
    except Exception as e:
        st.error(f"PDF 생성 중 오류 발생 ({creator_id}): {str(e)}")
        return None

def create_validation_excel(original_df, processed_df, creator_info_handler):
    """검증 결과를 담은 엑셀 파일을 생성합니다."""
    validator = DataValidator(original_df, creator_info_handler)
    
    summary_data = {
        '항목': ['전체 크리에이터 수', '총 조회수', '총 수익', '정산 후 총 수익'],
        '값': [
            validator.total_stats['creator_count'],
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data'],
            validator.total_stats['total_revenue_after']
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    
    validation_data = {
        '항목': ['총 조회수', '총 수익'],
        '원본 데이터': [
            validator.total_stats['total_views_data'],
            validator.total_stats['total_revenue_data']
        ],
        '처리 후 데이터': [
            processed_df['조회수'].sum(),
            processed_df['대략적인 파트너 수익 (KRW)'].sum()
        ]
    }
    validation_df = pd.DataFrame(validation_data)
    
    creator_comparison = validator.compare_creator_stats(processed_df)
    creator_comparison['수수료율'] = creator_comparison['아이디'].map(
        lambda x: creator_info_handler.get_commission_rate(x)
    )
    creator_comparison['수수료 후 수익'] = creator_comparison['대략적인 파트너 수익 (KRW)_processed'] * creator_comparison['수수료율']
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='전체 데이터 요약', index=False)
        validation_df.to_excel(writer, sheet_name='전체 데이터 검증', index=False)
        creator_comparison.to_excel(writer, sheet_name='크리에이터별 검증', index=False)
    
    excel_buffer.seek(0)
    return excel_buffer.getvalue()

def create_zip_file(reports_data, excel_files, original_df=None, processed_df=None, creator_info_handler=None):
    """보고서와 엑셀 파일들을 ZIP 파일로 압축합니다."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # HTML 보고서 및 PDF 추가
        for filename, content in reports_data.items():
            # HTML 파일 추가
            zip_file.writestr(f"reports/html/{filename}", content)
            
            # PDF 파일 생성 및 추가
            creator_id = filename.replace('_report.html', '')
            pdf_content = create_pdf_from_html(content, creator_id)
            if pdf_content:
                pdf_filename = filename.replace('.html', '.pdf')
                zip_file.writestr(f"reports/pdf/{pdf_filename}", pdf_content)
        
        # 엑셀 파일 추가
        for filename, content in excel_files.items():
            zip_file.writestr(f"excel/{filename}", content)
            
        # 검증 결과 엑셀 추가
        if all([original_df is not None, processed_df is not None, creator_info_handler is not None]):
            validation_excel = create_validation_excel(original_df, processed_df, creator_info_handler)
            zip_file.writestr("validation/validation_results.xlsx", validation_excel)
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def process_data(input_df, creator_info_handler, start_date, end_date, 
                send_email=False, email_user=None, email_password=None,  # 이메일 관련 매개변수 추가
                progress_container=None, status_container=None, validation_container=None):
    """데이터를 처리하고 보고서를 생성합니다."""
    reports_data = {}
    excel_files = {}
    processed_full_data = pd.DataFrame()
    failed_creators = []
    
    try:
        # 진행 상태 표시 초기화
        total_creators = len(creator_info_handler.get_all_creator_ids())
        if progress_container:
            progress_bar = progress_container.progress(0)
            progress_status = progress_container.empty()
            progress_text = progress_container.empty()
            failed_status = progress_container.empty()
            download_button = progress_container.empty()
            progress_status.write("처리 전")
        
        # 크리에이터별 처리
        for idx, creator_id in enumerate(creator_info_handler.get_all_creator_ids()):
            try:
                if progress_container:
                    progress_status.write("처리 중")
                    progress = (idx + 1) / total_creators
                    progress_bar.progress(progress)
                    progress_text.write(f"진행 상황: {idx + 1}/{total_creators} - {creator_id} 처리 중...")
                
                # 데이터 필터링 및 처리
                creator_data = input_df[input_df['아이디'] == creator_id].copy()
                if creator_data.empty:
                    failed_creators.append(creator_id)
                    continue
                
                # 데이터 처리
                creator_data['조회수'] = creator_data['조회수'].fillna(0)
                creator_data['대략적인 파트너 수익 (KRW)'] = creator_data['대략적인 파트너 수익 (KRW)'].fillna(0)
                commission_rate = creator_info_handler.get_commission_rate(creator_id)
                
                total_views = clean_numeric_value(creator_data['조회수'].sum())
                total_revenue_before = clean_numeric_value(creator_data['대략적인 파트너 수익 (KRW)'].sum())
                total_revenue_after = int(total_revenue_before * commission_rate)
                
                processed_full_data = pd.concat([processed_full_data, creator_data])
                
                # 상위 50개 데이터 필터링
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
                    'period': f"{start_date.strftime('%y.%m.%d')} - {end_date.strftime('%y.%m.%d')}",
                    'totalRevenueBefore': total_revenue_before,
                    'totalRevenue': total_revenue_after,
                    'totalViews': total_views,
                    'videoData': create_video_data(filtered_data[:-1])
                }
                
                # HTML 보고서 생성
                html_content = generate_html_report(report_data)
                if html_content:
                    reports_data[f"{creator_id}_report.html"] = html_content
                
                # 이메일 발송 처리
                if send_email and email_user and email_password:
                    try:
                        email = creator_info_handler.get_email(creator_id)
                        if email:
                            # SMTP 설정
                            smtp_server = "smtp.gmail.com"
                            smtp_port = 587
                            
                            # 이메일 메시지 생성
                            message = MIMEMultipart()
                            message["From"] = email_user
                            message["To"] = email
                            message["Subject"] = f"{creator_id} 크리에이터님의 음원 사용현황 보고서"
                            
                            body = f"""안녕하세요, {creator_id} 크리에이터님

                첨부된 파일을 통해 음원 사용현황을 확인해주세요.
                문의사항이 있으시면 언제든 연락 주시기 바랍니다.

                감사합니다."""
                            
                            message.attach(MIMEText(body, "plain"))
                            
                            # HTML 보고서 첨부
                            attachment = MIMEApplication(html_content.encode('utf-8'), _subtype="html")
                            attachment.add_header('Content-Disposition', 'attachment', 
                                                filename=f'{creator_id}_report.html')
                            message.attach(attachment)
                            
                            # 이메일 발송
                            with smtplib.SMTP(smtp_server, smtp_port) as server:
                                server.starttls()
                                server.login(email_user, email_password)
                                server.send_message(message)
                            
                            st.success(f"{creator_id} 크리에이터에게 이메일 발송 완료")
                        else:
                            st.warning(f"{creator_id} 크리에이터의 이메일 주소가 없습니다.")
                            
                    except Exception as e:
                        st.error(f"{creator_id} 크리에이터 이메일 발송 실패: {str(e)}")
                        failed_creators.append(creator_id)


            except Exception as e:
                failed_creators.append(creator_id)
                if status_container:
                    status_container.error(f"{creator_id} 크리에이터 처리 중 오류 발생: {str(e)}")
                continue
        
        # 모든 처리 완료 후 상태 업데이트
        if progress_container:
            progress_status.write("처리 완료")
            progress_text.write(f"진행 상황: {total_creators}/{total_creators} - 처리 완료")
            failed_status.write(f"실패: {', '.join(failed_creators) if failed_creators else 'None'}")
            
            if reports_data and excel_files:
                zip_data = create_zip_file(reports_data, excel_files, input_df, processed_full_data, creator_info_handler)
                download_button.download_button(
                    label="보고서 다운로드",
                    data=zip_data,
                    file_name="reports.zip",
                    mime="application/zip"
                )
        
        # 검증 결과 표시
        if not processed_full_data.empty and validation_container:
            with validation_container:
                show_validation_results(input_df, processed_full_data, creator_info_handler)
                
    except Exception as e:
        st.error(f"전체 처리 중 오류 발생: {str(e)}")
        st.write(traceback.format_exc())
        
    return reports_data, excel_files

def main():
    st.title("크리에이터 정산 보고서 생성기")
    
    with st.expander("📝 사용 방법", expanded=False):
        st.markdown("""
        ### 사용 방법
        1. 데이터 기간 설정
        2. 크리에이터 정보 파일(`creator_info.xlsx`) 업로드
        3. 통계 데이터 파일(`creator_statistics.xlsx`) 업로드
        4. 업로드된 데이터 검증 결과 확인
        5. 보고서 생성 버튼 클릭
        6. 처리 결과 검증 확인 후 보고서 다운로드
        """)
    
    # 파일 업로드 섹션
    st.header("1️⃣ 데이터 파일 업로드")

    # 데이터 기간 설정
    st.subheader("📅 데이터 기간 설정")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("시작일", format="YYYY-MM-DD")
    with col2:
        end_date = st.date_input("종료일", format="YYYY-MM-DD")

    creator_info = st.file_uploader("크리에이터 정보 파일 (creator_info.xlsx)", type=['xlsx'])
    statistics = st.file_uploader("통계 데이터 파일 (creator_statistics.xlsx)", type=['xlsx'])

    # 데이터 검증 섹션
    if creator_info and statistics:
        st.header("2️⃣ 데이터 검증")
        creator_info_handler = CreatorInfoHandler(creator_info)
        statistics_df = pd.read_excel(statistics, header=0)
        validator = DataValidator(statistics_df, creator_info_handler)
        
        # 데이터 검증 표시
        st.subheader("📊 전체 통계")
        comparison_data = {
            '항목': ['총 조회수', '총 수익'],
            '합계 행': [
                f"{validator.total_stats['total_views_summary']:,}",
                f"₩{validator.total_stats['total_revenue_summary']:,}"
            ],
            '실제 데이터': [
                f"{validator.total_stats['total_views_data']:,}",
                f"₩{validator.total_stats['total_revenue_data']:,}"
            ]
        }
        
        views_match = abs(validator.total_stats['total_views_summary'] - validator.total_stats['total_views_data']) < 1
        revenue_match = abs(validator.total_stats['total_revenue_summary'] - validator.total_stats['total_revenue_data']) < 1
        comparison_data['일치 여부'] = ['✅' if views_match else '❌', '✅' if revenue_match else '❌']
        
        comparison_df = pd.DataFrame(comparison_data)
        st.dataframe(
            comparison_df.style.apply(
                lambda x: ['background-color: #e6ffe6' if v == '✅' else 
                        'background-color: #ffe6e6' if v == '❌' else '' 
                        for v in x],
                subset=['일치 여부']
            ),
            use_container_width=True
        )
        
        # 보고서 생성 버튼
        st.header("3️⃣ 보고서 생성")
        if st.button("보고서 생성 시작", type="primary"):
            try:
                tab1, tab2 = st.tabs(["처리 진행 상황", "검증 결과"])
                
                with tab1:
                    progress_container = st.container()
                    status_container = st.container()
                
                with tab2:
                    validation_container = st.container()
                
                # 처리 시작
                with st.spinner('보고서 생성 중...'):
                    reports_data, excel_files = process_data(
                        statistics_df,
                        creator_info_handler,
                        start_date,
                        end_date,
                        send_email=send_email,
                        email_user=email_user,
                        email_password=email_password,
                        progress_container=progress_container,
                        status_container=status_container,
                        validation_container=validation_container
                    )
            
            except Exception as e:
                st.error(f"처리 중 오류가 발생했습니다: {str(e)}")
                st.write(traceback.format_exc())
        else:
            st.warning("필요한 파일을 모두 업로드해주세요.")


    # 이메일 설정 섹션
    st.header("3️⃣ 이메일 발송 설정")
    send_email = st.checkbox("보고서를 이메일로 발송하기")
    email_user = None
    email_password = None

    if send_email:
        st.info("""
        이메일 발송을 위해 Gmail 계정 설정이 필요합니다:
        1. Gmail 계정 (일반 구글 계정)
        2. 앱 비밀번호 생성 방법:
        - Google 계정 관리 → 보안 → 2단계 인증 → 앱 비밀번호
        - '앱 선택'에서 '기타' 선택 후 앱 비밀번호 생성
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            email_user = st.text_input("Gmail 계정", placeholder="example@gmail.com")
        with col2:
            email_password = st.text_input("Gmail 앱 비밀번호", type="password")


if __name__ == "__main__":
    main()

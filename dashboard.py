import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection

# 1. הגדרת עמוד (חייב להיות הפקודה הראשונה בקוד)
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# הזרקת CSS כדי שהכל יהיה מימין לשמאל (RTL) בצורה מסודרת
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    /* יישור כותרות וטקסטים לימין */
    h1, h2, h3, p, div, .stMarkdown {
        text-align: right;
    }
    /* תיקון ליישור של המטריקות */
    [data-testid="stMetricValue"] {
        direction: ltr; /* מספרים עדיף שישארו משמאל לימין */
        text-align: right;
    }
    [data-testid="stMetricLabel"] {
        text-align: right;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 דשבורד ניהול הזמנות")

# 2. פונקציה לטעינת נתונים עם Cache (כדי שלא יטען כל רגע מחדש)
@st.cache_data(ttl=600) # רענון נתונים כל 10 דקות
def load_data():
    conn = st.connection("gsheets", type=GSheetsConnection)
    # קריאת הנתונים - מושך הכל
    df = conn.read()
    
    # המרת עמודת התאריך לתאריך אמיתי של פייתון
    # dayfirst=True חשוב כי הפורמט שלנו הוא יום/חודש/שנה
    if 'תאריך' in df.columns:
        df['תאריך'] = pd.to_datetime(df['תאריך'], dayfirst=True, errors='coerce')
    
    return df

try:
    df = load_data()
    
    # יצירת עמודת עזר לתאריך בלבד (ללא שעה) לצורך הסינון
    if 'תאריך' in df.columns:
        df['date_only'] = df['תאריך'].dt.date

    # --- סרגל צד (Sidebar) ---
    st.sidebar.header("🔍 סינון וחיפוש")
    
    df_filtered = df.copy()

    # סינון לפי תאריכים
    if 'date_only' in df.columns:
        # מציאת תאריך מינימום ומקסימום מהקובץ
        min_date = df['date_only'].min()
        max_date = df['date_only'].max()
        
        if pd.notnull(min_date) and pd.notnull(max_date):
            st.sidebar.subheader("📅 טווח תאריכים")
            
            # פיצול לשני שדות נפרדים למראה נקי יותר
            col_date1, col_date2 = st.sidebar.columns(2) # אפשר גם אחד מתחת לשני, כאן שמתי בטורים צפופים או אחד מתחת לשני
            
            start_date = st.sidebar.date_input(
                "מתאריך:",
                value=min_date,
                min_value=min_date,
                max_value=max_date
            )
            
            end_date = st.sidebar.date_input(
                "עד תאריך:",
                value=max_date,
                min_value=min_date,
                max_value=max_date
            )
            
            if start_date > end_date:
                st.sidebar.error("⚠️ תאריך התחלה מאוחר מתאריך סיום")
            else:
                # ביצוע הסינון בפועל
                mask_date = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df_filtered.loc[mask_date]

    # קו מפריד
    st.sidebar.markdown("---") 

    # חיפוש חופשי
    search_term = st.sidebar.text_input("🔎 חיפוש חופשי", placeholder="שם לקוח, מק\"ט, עיר...")
    if search_term:
        # מסנן שורות שבהן הטקסט מופיע באחת העמודות
        mask_search = df_filtered.astype(str).apply(
            lambda x: x.str.contains(search_term, case=False, na=False)
        ).any(axis=1)
        df_filtered = df_filtered[mask_search]

    # --- תצוגת מדדים (KPIs) ---
    # חישובים על הדאטה המסונן
    total_rows = len(df_filtered)
    
    # לוגיקה: אם אין מספר משלוח (ריק) = התקנה, אחרת = הזמנה רגילה
    if 'מספר משלוח' in df_filtered.columns:
        installs_count = df_filtered['מספר משלוח'].isna().sum()
        regular_count = df_filtered['מספר משלוח'].notna().sum()
    else:
        installs_count = 0
        regular_count = total_rows

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 סה\"כ רשומות", total_rows)
    col2.metric("🚛 הזמנות רגילות", regular_count)
    col3.metric("🛠️ התקנות", installs_count)

    st.divider()

    # --- תצוגת הטבלה ---
    st.subheader(f"רשימת הזמנות ({len(df_filtered)} תוצאות)")
    
    # הסתרת עמודת העזר 'date_only' לפני התצוגה כדי לא לבלבל
    display_df = df_filtered.drop(columns=['date_only'], errors='ignore')

    # עיצוב תאריך לתצוגה יפה (DD/MM/YYYY)
    if 'תאריך' in display_df.columns:
        display_df['תאריך'] = display_df['תאריך'].dt.strftime('%d/%m/%Y')

    st.dataframe(
        display_df, 
        use_container_width=True,
        hide_index=True,
        height=600
    )

except Exception as e:
    st.error(f"שגיאה בטעינת הנתונים: {e}")
    st.info("💡 טיפ: וודא ששמות העמודות בקובץ השיטס תואמים (במיוחד 'תאריך' ו-'מספר משלוח').")

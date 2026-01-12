import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection

# 1. הגדרת עמוד (חייב להיות שורה ראשונה)
st.set_page_config(
    page_title="דשבורד הזמנות",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# כותרת ועיצוב RTL
st.markdown("""
<style>
    .stApp {
        direction: rtl;
        text-align: right;
    }
    /* התאמה לכותרות */
    h1, h2, h3, p, div {
        text-align: right;
    }
</style>
""", unsafe_allow_html=True)

st.title("📊 דשבורד ניהול הזמנות")

# 2. התחברות לגוגל שיטס
# אנחנו משתמשים ב-cache כדי שלא יקרא כל שנייה מחדש את הקובץ אלא רק כשיש שינוי או אחרי זמן מה
@st.cache_data(ttl=600) # רענון כל 10 דקות
def load_data():
    conn = st.connection("gsheets", type=GSheetsConnection)
    # קריאת הנתונים
    df = conn.read(usecols=list(range(10))) # קורא את 10 העמודות הראשונות ליתר ביטחון
    
    # המרת עמודת התאריך לתאריך אמיתי
    if 'תאריך' in df.columns:
        df['תאריך'] = pd.to_datetime(df['תאריך'], dayfirst=True, errors='coerce')
    
    return df

try:
    df = load_data()
    
# 3. סרגל צד לסינונים
    st.sidebar.header("🔍 סינון נתונים")
    
    # בדיקה שיש עמודת תאריך והיא תקינה
    if 'תאריך' in df.columns:
        # המרה בטוחה לתאריך (ללא שעה) לצורך ה-Widget
        df['date_only'] = df['תאריך'].dt.date
        
        min_date = df['date_only'].min()
        max_date = df['date_only'].max()
        
        if pd.notnull(min_date) and pd.notnull(max_date):
            st.sidebar.subheader("📅 טווח תאריכים")
            
            # פיצול לשני שדות נפרדים - יותר אסתטי בסרגל צד
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
            
            # בדיקת תקינות (שההתחלה לא אחרי הסוף)
            if start_date > end_date:
                st.sidebar.error("תאריך התחלה חייב להיות לפני תאריך סיום")
                df_filtered = df # במקרה של שגיאה לא מסננים או שמציגים ריק
            else:
                # סינון הדאטה
                mask = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
                df_filtered = df.loc[mask]
        else:
            df_filtered = df
    else:
        df_filtered = df

    # חיפוש חופשי (לפי לקוח, מק"ט או כל דבר אחר)
    st.sidebar.markdown("---") # קו מפריד
    search_term = st.sidebar.text_input("🔎 חיפוש חופשי", placeholder="שם לקוח / פריט...")
    
    if search_term:
        # מחפש את הטקסט בכל העמודות
        mask_search = df_filtered.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
        df_filtered = df_filtered[mask_search]

    # 4. הצגת מטריקות (KPIs)
    col1, col2, col3 = st.columns(3)
    
    total_orders = len(df_filtered)
    # ניסיון לחשב סה"כ התקנות (אם מספר משלוח ריק = התקנה)
    total_installs = df_filtered['מספר משלוח'].isna().sum() if 'מספר משלוח' in df_filtered.columns else 0
    regular_orders = total_orders - total_installs
    
    col1.metric("📦 סה\"כ רשומות (בסינון)", total_orders)
    col2.metric("🚛 הזמנות רגילות", regular_orders)
    col3.metric("🛠️ התקנות", total_installs)

    st.divider()

    # 5. הצגת הטבלה
    st.subheader("📋 פירוט הזמנות")
    
    # מציג את הטבלה בצורה אינטראקטיבית
    st.dataframe(
        df_filtered, 
        use_container_width=True,
        hide_index=True,
        height=600
    )

except Exception as e:
    st.error(f"שגיאה בטעינת הנתונים: {e}")
    st.info("אנא וודא שהקובץ בדרייב מוגדר עם הרשאות עריכה לבוט ושהסודות מוגדרים נכון.")

import streamlit as st
from utils import main
import pandas as pd

if __name__ == "__main__":

    st.title('CDA Assignment "Logistic Regression"')
    st.write('BTS')
    st.write('Roberto Arenal')
    #df=main().plot()

    st.subheader('Distribution of target variable:')
    plots = main()
    fig = plots.plot_target()
    st.pyplot(fig)

    st.subheader('Variables Analysis:')
    fig2 = plots.plot_count('job')
    st.pyplot(fig2)

    fig3 = plots.plot_count('marital')
    st.pyplot(fig3)
    #st.write(fig3)

    fig4 = plots.plot_count('education')
    st.pyplot(fig4)

    fig5 = plots.plot_count('day_of_week')
    st.pyplot(fig5)

    fig6 = plots.plot_count('month')
    st.pyplot(fig6)

    fig7 = plots.plot_count('poutcome')
    st.pyplot(fig7)

    st.subheader('Train Model(unbalanced):')
    df1, acc, recall, prec, confm = plots.log_reg()
    st.write(df1)
    st.write('Accuracy: ', acc, '    Recall: ', recall, '    Precision: ', prec)
    # st.write('Recall: ', recall)
    # st.write('Precision: ', prec)
    st.write('Confusion Matrix: ', confm)

    st.subheader('Train Model(balanced):')
    df2, acc2, recall2, prec2, conf2 = plots.log_reg_b()
    st.write(df2)
    st.write('Accuracy: ', acc2, '    Recall: ', recall2, '    Precision: ', prec2)
    st.write('Confusion Matrix: ', conf2)

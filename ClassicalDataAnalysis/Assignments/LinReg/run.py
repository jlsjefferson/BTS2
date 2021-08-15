import streamlit as st
from utils import main
import pandas as pd

if __name__ == "__main__":

    st.title('CDA Assignment "Linear Regression"')
    st.write('BTS')
    st.write('Roberto Arenal')
    #df=main().plot()

    st.subheader('Distribution of target variable:')
    plots = main()
    fig = plots.plot_hist()
    st.pyplot(fig)

    st.subheader('Correlation heatmap:')
    fig2 = plots.plot_heat()
    st.pyplot(fig2)

    st.subheader('Linear Regression:')
    option = st.radio('Select features:',['Time on Website', 'Time on App'], index=0)
    fig3, rmse_train, r2_train, rmse_test, r2_test = plots.linear_reg(option)
    st.pyplot(fig3)
    st.write('RMSE   ', 'trining set: ',rmse_train, 'test set: ',rmse_test)
    st.write('R2 score    ','training set: ', r2_train, 'test set:  ',r2_test)

    st.subheader('Multivariate Linear Regression')
    options = st.multiselect('Select features:',['Avg. Session Length','Time on Website', 'Time on App','Length of Membership'],
                                ['Time on App','Avg. Session Length','Length of Membership'])
    if st.button('Execute'):
        rmse_train, r2_train, rmse_test, r2_test, coef = plots.linear_reg_mult(options)
        coef=pd.DataFrame(coef, columns=options)
        st.write('Coefficients: ',coef.T)
        st.write('RMSE   ', 'trining set: ',rmse_train, 'test set: ',rmse_test)
        st.write('R2 score    ','training set: ', r2_train, 'test set:  ',r2_test)

    st.sidebar.write('The target variable is normaly distributed. And there is not need to remove outliers.')
    st.sidebar.markdown('**The company should focuss on the App since the web page doesnt contribute positively to the Sales.**')
    st.sidebar.write('The Length of Membership is the feature that impacts Sales the most')
    st.sidebar.write('Coefficients Interpretation:')
    st.sidebar.write('- With all other variables held constant, if the length of membership increase by 1, Sales will increase by 61.68' )
    st.sidebar.write('- With all other variables held constant, if the time on app increase by 1, sales will increase by 38.27')
    st.sidebar.write('- With all other variables held constant, if the avg. session length increase by 1, sales will increase by 25.66')
    st.sidebar.write('Before investing in the app, the company should invest on developing a strategy to mantain their actual members and increase their loyalty. This because what impact sales the most is the Length of membership, meaning that loyal customers are the ones who buy more. Later on, the company can invest on the app. Doing something to increase the amount of time that the user spents. For example, providing a section of recommended items based on searches that he/she has made.')

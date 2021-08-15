import os
import pathlib
import urllib.request
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from imblearn.over_sampling import SMOTE


class DataFetch(object):

    def __init__(self):
        self.DOWNLOAD_ROOT = "https://raw.githubusercontent.com/vfp1/bts-cda-2020/main/Session_2/banking.csv"
        self.DS_PATH = os.path.join("dataset")
        #self.TEAMS_URL = self.DOWNLOAD_ROOT + "datasets/nfl_teams.csv"

    def fetch_data(self):
        if not os.path.isdir(self.DS_PATH):
            os.makedirs(self.DS_PATH)

        save_path = os.path.join(self.DS_PATH,'banking.csv')
        urllib.request.urlretrieve(self.DOWNLOAD_ROOT,save_path)

class main(object):

    def __init__(self):
        self.DS_PATH = os.path.join("dataset")
        csv_path = os.path.join(self.DS_PATH, 'banking.csv')
        self.banking = pd.read_csv(csv_path)
#        self.banking = self.banking.loc[(self.banking['education']=='basic.4y') | (self.banking['education']=='basic.6y') | (self.banking['education']=='basic.9y'),'education'] = 'basic'

    def _split_data(self,X,Y):

        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.3, random_state=5)

        return X_train, X_test, Y_train, Y_test

    def _get_dummies(self,df,cols):
        df = pd.get_dummies(df, columns=cols,drop_first=True)

        return df

    def _smote(self,df):
        X = df.loc[:, df.columns != 'y']
        y = df.loc[:, df.columns == 'y']
        os = SMOTE(random_state=0)
        X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.30, random_state=0)
        os_data_X,os_data_y= os.fit_resample(X_train, y_train)

        return os_data_X,os_data_y

    def plot_target(self):
        sns.set_theme(style="white")
        fig = plt.figure(figsize=(12,6))
        ax = sns.countplot(x="y", data=self.banking)

        for p, label in zip(ax.patches, self.banking.groupby(['y'])['y'].count()):
            ax.annotate(label, (p.get_x()+0.375, p.get_height()+0.15))
            print(label)

        plt.title('Bar')
        plt.xlabel('Got Deposit')
        plt.ylabel('Customers')
        plt.show()

        return fig


    def plot_count(self,col):
        sns.set_theme(style="white")
        fig = plt.figure(figsize=(12,6))
        sns.countplot(y=col, hue="y", data=self.banking)
        #plt.title('Purchase Frequency for Job Title')
        plt.xlabel('Customers')
        plt.ylabel(col)
        plt.show()

        return fig

    def plot_percent(self,col):
        fig=self.banking.groupby(col)['y'].value_counts(normalize=True).mul(100).rename('percent').reset_index().pipe((sns.catplot,'data'), x=col,y='percent',hue='y',kind='bar',height=4)

        return fig

    def log_reg(self):
        df = self.banking.copy()
        df.loc[(df['education']=='basic.4y') | (df['education']=='basic.6y') | (df['education']=='basic.9y'),'education'] = 'basic'

        #Check the counts and percentage of y
        s = df.y
        counts = s.value_counts()
        percent = s.value_counts(normalize=True).mul(100).round(2).astype(str) #+ '%'
        df1 = pd.DataFrame({'y':s.unique(),'counts': counts, 'per': percent})

        #Get object columns to pass to dummies function.
        cols = list(df.select_dtypes(include = 'object').columns)
        df = self._get_dummies(df,cols)

        X =  df.drop('y', axis=1)
        Y = df.loc[:, df.columns == 'y']
        #Y = df.iloc[:,-1]

        X_train, X_test, Y_train, Y_test = self._split_data(X,Y)

        logmodel = LogisticRegression()
        logmodel.fit(X_train,Y_train)
        y_pred = logmodel.predict(X_test)
        acc = "{:.2%}".format(logmodel.score(X_test, Y_test))
        #rep = metrics.classification_report(Y_test,y_pred)
        recall = "{:.2%}".format(metrics.recall_score(Y_test, y_pred))
        prec = "{:.2%}".format(metrics.precision_score(Y_test,y_pred))
        confm = metrics.confusion_matrix(Y_test,y_pred)

        return df1, acc, recall, prec, confm

    def log_reg_b(self):
        df = self.banking.copy()
        df.loc[(df['education']=='basic.4y') | (df['education']=='basic.6y') | (df['education']=='basic.9y'),'education'] = 'basic'

        #Get object columns to pass to dummies function.
        cols = list(df.select_dtypes(include = 'object').columns)
        df = self._get_dummies(df,cols)

        X,Y = self._smote(df)

        #Check the counts and percentage of y
        k=pd.DataFrame(Y,columns=['y'])
        s = k.y
        counts = s.value_counts()
        percent = s.value_counts(normalize=True).mul(100).round(2).astype(str) #+ '%'
        df1 = pd.DataFrame({'y':s.unique(),'counts': counts, 'per': percent})

        X_train, X_test, Y_train, Y_test = self._split_data(X,Y)

        logmodel = LogisticRegression()
        logmodel.fit(X_train,Y_train)
        y_pred = logmodel.predict(X_test)
        acc = "{:.2%}".format(logmodel.score(X_test, Y_test))
        recall = "{:.2%}".format(metrics.recall_score(Y_test, y_pred))
        prec = "{:.2%}".format(metrics.precision_score(Y_test,y_pred))
        confm = metrics.confusion_matrix(Y_test,y_pred)

        return df1, acc, recall, prec, confm

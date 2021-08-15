import os
import pathlib
import urllib.request
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.linear_model import LinearRegression

class DataFetch(object):

    def __init__(self):
        self.DOWNLOAD_ROOT = "https://raw.githubusercontent.com/vfp1/bts-cda-2020/main/Session_1/Ecommerce_Customers.csv"
        self.DS_PATH = os.path.join("dataset")
        #self.TEAMS_URL = self.DOWNLOAD_ROOT + "datasets/nfl_teams.csv"

    def fetch_data(self):
        if not os.path.isdir(self.DS_PATH):
            os.makedirs(self.DS_PATH)

        save_path = os.path.join(self.DS_PATH,'Ecommerce_Customers.csv')
        #urllib.request.urlretrieve(self.TEAMS_URL, teams_path)
        urllib.request.urlretrieve(self.DOWNLOAD_ROOT,save_path)

class main(object):

    def __init__(self):
        self.DS_PATH = os.path.join("dataset")
        csv_path = os.path.join(self.DS_PATH, 'Ecommerce_Customers.csv')
        self.customers = pd.read_csv(csv_path)

    def _split_data(self,X,Y):

        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.25, random_state=5)

        return X_train, X_test, Y_train, Y_test

    def plot_hist(self):
        sns.set_theme(style="white")
        fig = plt.figure(figsize=(12,6))
        #f, ax = plt.subplots(figsize=(10, 9))
        sns.histplot(self.customers['Yearly Amount Spent'], kde=True,bins=int(180/5))
        #plt.title('Distribution of target variable')
        plt.ylabel('Customers')
        #return customers.head()
        return fig

    def plot_heat(self):

        corr1=self.customers.corr()
        mask = np.triu(np.ones_like(corr1, dtype=bool))
        #sns.set_theme(style="white")
        fig = plt.figure(figsize=(12,6))
        cmap = sns.diverging_palette(230, 20)
        heatmap=sns.heatmap(corr1, mask=mask, cmap=cmap, vmin=-1, vmax=1, center=0, square=True, linewidths=.5, cbar_kws={"shrink": .5}, annot=True)
        #heatmap.set_title('Correlation Heatmap')

        return fig

    def linear_reg(self,X):
        #X = self.customers[['Time on App']]
        name=X
        X = self.customers[[X]]
        Y = self.customers[['Yearly Amount Spent']]
        X_train, X_test, Y_train, Y_test = self._split_data(X,Y)
        lin_model = LinearRegression()
        lin_model.fit(X_train, Y_train)
        #plt.figure(figsize=(12,6))
        #fig = plt.figure()
        fig, ax = plt.subplots(2,figsize=(10, 6))
        ax[0].set_title('Train set')
        ax[0].set_ylabel('Yearly Sales Amount')
        ax[0].scatter(X_train, Y_train, s=10)
        ax[1].set_title('Test set')
        ax[1].set_xlabel(name)
        ax[1].set_ylabel('Yearly Sales Amount')
        ax[1].scatter(X_test, Y_test, s=10)
        y_pred_train = lin_model.predict(X_train)
        y_pred_test = lin_model.predict(X_test)
        ax[0].plot(X_train, y_pred_train, color = "green")
        ax[1].plot(X_test, y_pred_test, color = "green")
        rmse_train = np.sqrt(mean_squared_error(Y_train, y_pred_train))
        r2_train = r2_score(Y_train, y_pred_train)
        rmse_test = np.sqrt(mean_squared_error(Y_test, y_pred_test))
        r2_test = r2_score(Y_test, y_pred_test)


        return fig, rmse_train, r2_train, rmse_test, r2_test
        #return (lin_model.coef_,lin_model.intercept_)

    def linear_reg_mult(self,X):
        X = self.customers[X]
        Y = self.customers[['Yearly Amount Spent']]
        X_train, X_test, Y_train, Y_test = self._split_data(X,Y)
        lin_model = LinearRegression()
        lin_model.fit(X_train, Y_train)
        y_pred_train = lin_model.predict(X_train)
        y_pred_test = lin_model.predict(X_test)
        rmse_train = np.sqrt(mean_squared_error(Y_train, y_pred_train))
        r2_train = r2_score(Y_train, y_pred_train)
        rmse_test = np.sqrt(mean_squared_error(Y_test, y_pred_test))
        r2_test = r2_score(Y_test, y_pred_test)

        return rmse_train, r2_train, rmse_test, r2_test,lin_model.coef_

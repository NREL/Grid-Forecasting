# ~~~~~~~~~~~~ Author: Mengmeng Cai ~~~~~~~~~~~~~~~

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

class Plot_helper(object):

    def __init__(self, MainDir):
        """ Function used for initializing the Plot_helper object

        Args:
            MainDir: Main directory of the DLMP project
        """

        self.MainDir = MainDir
        self.result_dir = os.path.join(self.MainDir,'Result')


    def processing(self, raw, name, toname):
        """ Helper function used for rearranging raw data according to phases

        Args:
            raw: Raw data to be processed, Dataframe
            name: column name of the data to be processed, str
            toname: new column name header to be stored in the processed dataset, str

        Return:
            processed: processed data that have been rearranged by phases
        """

        # tem = raw.index[0][:-2]
        # bus_list = [tem]
        # for idx in raw.index:
        #     if idx[:-2] == tem:
        #         pass
        #     else:
        #         bus_list.append(idx[:-2])
        #         tem = idx[:-2]
        bus_list = set([idx[:-2] for idx in raw.index])
        processed = pd.DataFrame(np.nan, index=bus_list, columns=[toname+'_'+'1', toname+'_'+'2', toname+'_'+'3'])

        for idx in raw.index:
            processed[toname+'_'+idx[-1:]][idx[:-2]] = raw[name][idx]
        return processed

    def plotting(self, dataset, hold, ylabel):
        """ Helper function used for plotting the input dataset by phases

        Args:
            dataset: dataset to be plotted, Dataframe
            hold: whether to hold the show function or not, Boolean
            ylabel: name of the y axis label, str
        """

        sns.set()
        sns.set_style('dark')
        if hold == True:
            p = sns.lineplot(data=dataset, palette='colorblind', linewidth=1.5, sort=False)
            p.xaxis.set_major_locator(ticker.MultipleLocator(10))
        else:
            p = sns.lineplot(data=dataset, palette='deep', linewidth=1.5, sort=False)
            p.xaxis.set_major_locator(ticker.MultipleLocator(10))

        plt.xlabel('Bus')
        plt.ylabel(ylabel)
        # plt.show()
        fig = p.get_figure()
        title = ylabel.replace('$','price').replace(' ','_').replace('/',' per ')
        fig.savefig(os.path.join(self.result_dir,title), bbox_inches='tight')
        # print(os.path.join(self.result_dir,title))
        plt.clf()

    def plot_DLMPs(self, sorted_list=None):
        """ Function used for plotting the integrated DLMPs and decomposed DLMPs
        """

        DLMP_P = pd.read_csv(self.MainDir+'/Result/DLMP_P.csv', index_col=0)
        DLMP_P_B = pd.read_csv(self.MainDir+'/Result/DLMP_P_B.csv', index_col=0)
        DLMP_P_Vmag = pd.read_csv(self.MainDir+'/Result/DLMP_P_Vmag.csv', index_col=0)
        DLMP_Q = pd.read_csv(self.MainDir+'/Result/DLMP_Q.csv', index_col=0)
        DLMP_Q_B = pd.read_csv(self.MainDir+'/Result/DLMP_Q_B.csv', index_col=0)
        DLMP_Q_Vmag = pd.read_csv(self.MainDir+'/Result/DLMP_Q_Vmag.csv', index_col=0)
        if sorted_list:
            DLMP_P = DLMP_P.reindex(sorted_list)
            DLMP_P_B = DLMP_P_B.reindex(sorted_list)
            DLMP_P_Vmag = DLMP_P_Vmag.reindex(sorted_list)
            DLMP_Q = DLMP_Q.reindex(sorted_list)
            DLMP_Q_B = DLMP_Q_B.reindex(sorted_list)
            DLMP_Q_Vmag = DLMP_Q_Vmag.reindex(sorted_list)


        DLMP_P_pro = self.processing(DLMP_P, 'DLMP_P', 'DLMP_P').interpolate()
        DLMP_P_B_pro = self.processing(DLMP_P_B, 'DLMP_P_B', 'DLMP_P_B').interpolate()
        DLMP_P_Vmag_pro = self.processing(DLMP_P_Vmag, 'DLMP_P_Vmag', 'DLMP_P_Vmag').interpolate()
        DLMP_Q_pro = self.processing(DLMP_Q, 'DLMP_Q', 'DLMP_Q').interpolate()
        DLMP_Q_B_pro = self.processing(DLMP_Q_B, 'DLMP_Q_B', 'DLMP_Q_B').interpolate()
        DLMP_Q_Vmag_pro = self.processing(DLMP_Q_Vmag, 'DLMP_Q_Vmag', 'DLMP_Q_Vmag').interpolate()

        self.plotting(DLMP_P_pro, False, ylabel='Active power DLMP ($/kW)')
        self.plotting(DLMP_P_B_pro, False, ylabel='Active power DLMP contributed by load balance ($/kW)')
        self.plotting(DLMP_P_Vmag_pro, False, ylabel='Active power DLMP contributed by Vmag inequality constraint ($/kW)')
        self.plotting(DLMP_Q_pro, False, ylabel='Reactive power DLMP ($/kW)')
        self.plotting(DLMP_Q_B_pro, False, ylabel='Reactive power DLMP contributed by load balance ($/kW)')
        self.plotting(DLMP_Q_Vmag_pro, False, ylabel='Reactive power DLMP contributed by Vmag inequality constraint ($/kW)')

        return DLMP_P_pro, DLMP_P_B_pro, DLMP_P_Vmag_pro, DLMP_Q_pro, DLMP_Q_B_pro, DLMP_Q_Vmag_pro

    def plot_Vmag(self):
        """ Function used for plotting the voltage magnitudes before and after adding DGs to the network
        """

        Vmag = pd.read_csv(self.MainDir+'/Result/Vmag.csv', index_col=0)
        NonDG_Vmag = pd.read_csv(self.MainDir+'/Result/NonDG_Vmag.csv', index_col=0)
        Vmag_pro = self.processing(Vmag, 'Vmag', 'Vmag').interpolate()
        NonDG_Vmag_pro = self.processing(NonDG_Vmag, 'NonDG_Vmag', 'NonDG_Vmag').interpolate()
        self.plotting(Vmag_pro, True, ylabel='Voltage magnitude (pu)')
        self.plotting(NonDG_Vmag_pro, False, ylabel='Voltage magnitude (pu)')

    def plot_Vhat_convergence(self, num_iterations=10,sorted_list=None):
        """ Function used for plotting the convergence of Vhat
        """

        Vhat = pd.read_csv(self.MainDir+'/intermediary_result/Vhat.csv', index_col=0)
        if sorted_list:
            Vhat = Vhat.reindex(sorted_list)
        dist = np.empty([len(Vhat.index), num_iterations-1], dtype=np.complex_)
        dist[:] = np.nan
        for i in range(len(Vhat.index)):
            for j in range(num_iterations-1):
                dist[i, j] = complex(Vhat.values[i, j+1])-complex(Vhat.values[i, j])

        dist = np.absolute(dist).T
        dist = pd.DataFrame(dist, index=range(num_iterations-1), columns=Vhat.index)
        for col in dist.columns:
            plt.plot(range(1, num_iterations), dist[col])
        plt.ylabel('Distance')
        plt.xlabel('Iteration')
        title = 'vhat_convergence'
        plt.savefig(os.path.join(self.result_dir, title), bbox_inches='tight')
        # plt.show()
        plt.clf()
#We want to calculate the POP of the options which is important to strategy calculation
#https://medium.com/@rgaveiga/probability-of-profit-of-an-options-strategy-from-the-black-scholes-model-6146585f0fa0

#Another example
#https://www.interactivebrokers.com/en/general/education/probability_lab.php 


#1) Reverse Engineer Black Scholes Model to get POP
#https://www.codearmo.com/python-tutorial/options-trading-black-scholes-model
#black scholes
import numpy as np
from scipy.stats import norm

N = norm.cdf

# def BS_CALL(S, K, T, r, sigma):
#     d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return S * N(d1) - K * np.exp(-r*T)* N(d2)

# def BS_PUT(S, K, T, r, sigma):
#     d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
#     d2 = d1 - sigma* np.sqrt(T)
#     return K*np.exp(-r*T)*N(-d2) - S*N(-d1)


def get_d2(S, K, T, r, sigma):
    d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return d2


# Import get_d1_d2() and scipy's statistical module

# Get d2 for both the lower and upper bounds of the PR
d2_lower=get_d2(100,80,60/365,0.01,0.2)
d2_upper=get_d2(100,120,60/365,0.01,0.2)

#We have calculate the probability (assuming normal distribution and other black-scholes assumptions) that the price of the stock
#will stay between strike 1 and strike 2


# Calculate the PoP from the difference in the CDFs of d2 for the lower 
# and upper bounds of the PR
pop=N(d2_lower)-N(d2_upper)

# Print the result 
print("Probability of Profit (PoP) is %.2f%%" % (pop*100))



#2) Use the second method to create a probaility distribution of stock price and get a POP
from database_test import connect, close_connection

def get_call_strikes_and_prices(expiry, ticker):
    pass






#3) Use the binomail Model to calculate POP



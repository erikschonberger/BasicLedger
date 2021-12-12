import pandas as pd
from pandasql import sqldf as ps



class Ledger:
    def __init__(self):
        self.interestPayable=0
        self.payedInterest=0
        self.advanceIdent=1
        self.accruedPayment=0
    def getInterestPayable(self):
        return self.interestPayable
    def setInterestPayable(self,amount):
        self.interestPayable+=amount
    def getPayedInterest(self):
        return self.payedInterest
    def setPayedInterest(self,amount):
        self.payedInterest+=amount
    def setAggPay(self,amount):
        self.accruedPayment+=amount
    def getAggPay(self):
        return self.accruedPayment
    
    def loadEvents(self,dataLocation):
        global Event
        global advances
        global payments
        #load all events into base table
        baseEventsRead=pd.read_csv(dataLocation,header=None)
        Events=baseEventsRead.set_axis(["Event","Date","Amount"],axis=1)
        
        #load advances into advance table
        advances=ps("select Date,Amount as BaseAmount,Amount as CurrentAmount from Events where Event== 'advance'")
        advances.insert(0,"AdvanceID",range(len(advances)))
        
        #load payments into payment table
        payments=ps("select Date,Amount from Events where Event== 'payment'")
        payments.insert(0,"paymentID",range(len(payments)))
        
    def calculateInterest(self,startDate,endDate):
        #given date calculate interest for all advances before endDate, that 
        interestAdded=0
        datediff= (endDate-startDate).days
        endDateFormatted =pd.to_datetime(endDate,format='%Y-%m-%d')
        
        #calculate amt of days each adv is in the period
        #gives df of advances that existed before previous payment, so we can just calc based on time since then
        prePayDays=ps("select CurrentAmount from advances where Date <= '{}' and CurrentAmount > 0".format(startDate))
        prePayInt = prePayDays.sum()[0]*.00035*datediff
        
        #gets df of advances made after previous payment and before current payment
        postPayDays=ps("select * from advances where Date > '{}' and Date <= '{}'  and CurrentAmount > 0".format(startDate,endDate))
        endDateFormatted
        postPayDays['Date']=  pd.to_datetime(postPayDays['Date'], format='%Y-%m-%d')
        postPayDays['DateDiff']=endDateFormatted-postPayDays['Date']
        
        #gets interest for advances made between previous loan and current
        postPayDays['interest']=postPayDays['CurrentAmount']*.00035*((postPayDays['DateDiff'].astype('timedelta64[D]')).astype(int))
        postPayInt= postPayDays['interest'].sum()
        
        interestAdded=postPayInt+prePayInt
        return interestAdded
    def calculateSheet(self):
        global Event
        global advances
        global payments
        for payment in range(len(payments)):
            #get payments in order
            # calculate interest for payment date
            if payment > 0 :
                self.setInterestPayable(self.calculateInterest(pd.to_datetime(list(payments.loc[payments['paymentID'].eq(payment-1),'Date'])[0]),pd.to_datetime(list(payments.loc[payments['paymentID'].eq(payment),'Date'])[0])))
            else:
                self.setInterestPayable(self.calculateInterest(pd.to_datetime(list(advances.loc[advances['AdvanceID'].eq(0),'Date'])[0]),pd.to_datetime(pd.to_datetime(list(payments.loc[payments['paymentID'].eq(payment),'Date'])[0]))))
            #print("current interest is {}".format(self.getInterestPayable()))
            #if payment is greater than current interest, pay advances (events aren't dynamic so i would pay them as they came in, including Future advances)
            payAmt=list(payments.loc[payments['paymentID'].eq(payment),'Amount'])[0]
            if payAmt > self.getInterestPayable():
                #print("amount to pay before interest {}".format(payAmt))
                #set amount payed to total payable interest, then reduce current interest from payment, then set interest payable to 0 
                self.setPayedInterest(self.getInterestPayable())
                payAmt-=self.getInterestPayable()
                self.setInterestPayable(-self.getInterestPayable())
                #print("payed off interest, amt left {}".format(payAmt))
                for advance in range(len(advances)):
                    currentBalancePay=list(advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])
                    if payAmt > currentBalancePay[0]:
                        payAmt-=currentBalancePay[0]
                        #advance.currentAmt = 0
                        #print("current balance of advance before payment {}".format(list(advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])[0]))
                        advances.loc[advances['AdvanceID'] == (advance), 'CurrentAmount']=0
                        #print("current balance of advance after payment {}".format(list(advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])[0]))
                    else:
                        #advance.currentAmt = 0
                        #print("current balance of advance before payment {}".format(list(advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])[0]))
                        advances.loc[advances['AdvanceID'] == (advance), 'CurrentAmount']=((advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])[0]-payAmt)
                        #print("current balance of advance after payment {}".format(list(advances.loc[advances['AdvanceID'].eq(advance),'CurrentAmount'])[0]))
                        payAmt=0
                        #payment is 0, no reason to continue loop
                        break
                        
                #i have payed ALL advances, any payments now become accrued
                if payAmt != 0:
                    self.setAggPay(payAmt)
                else:
                    #payment is less than interest, just pay off interest
                    self.setPayedInterest(payAmt)
                    self.setInterestPayable(-payAmt)
        #in case last event is an advance, ensure interest is still calculate after all payments are done
        maxAdvDate=pd.to_datetime(list(advances.loc[advances['AdvanceID'].eq(len(advances)-1),'Date'])[0])
        maxPayDate=pd.to_datetime(list(payments.loc[payments['paymentID'].eq(len(payments)-1),'Date'])[0])
        if(maxPayDate < maxAdvDate):
            self.setInterestPayable(self.calculateInterest(maxPayDate,maxAdvDate))
        print(advances.to_string(index=False))
        print("Aggregate Advance Balance: {}".format(ps("select CurrentAmount from advances ").sum()[0]))
        print("Interest Payable Balance: {}".format(self.getInterestPayable()))
        print("Total Interest Paid: {}".format(self.getPayedInterest()))
        print("Accrued payments to future advances: {}".format(self.getAggPay()))
        
            
        return
        
test = Ledger()
Events = pd.DataFrame()
advances = pd.DataFrame()
payments = pd.DataFrame()

dataloaded=False
while True:
    input1=input("please enter a command (load/balances/exit): ")
    if input1=="exit":
        break
    if input1=="load":
        input2=input("please enter data to load")
        test.loadEvents(input2)
        dataloaded=True
    elif input1=="balances":
        if dataloaded:
            test.calculateSheet()
        else:
            print("no data loaded, cannot display balance")
    else:
        print("Invald command!")

#read in csv, convert to two tables
#table 0: events
#type,date,amt

#table 1: advances
#ident,baseDate,base amt,current amt

#table 2: payments
#order,date,amt

#for each loan:
#    add to table
#    set current amt to base amt
#    subtract accrued payments from curent amt
    

#for each payment:
#    calculate interest on loan(determine loan length in period for each loan)
#    subtract interest from payment(move to interest payed)
#    if Payment <= total loans
#        subtract payment from oldest NON ZERO loan
#        repeat until payment left is zero
#    else
#        after loans paid, add payment to accrued payments
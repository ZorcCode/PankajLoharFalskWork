from flask import Flask, request
# To load as a list of JSON
import pandas as pd



#setting up our data base
url = 'https://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json'
data = pd.read_json(url, lines=True, orient='columns')
db = pd.DataFrame(data)
#creating lists to store values after first query fired
lidisc = []
libr= []
lidisc2 = []
libr2= []
count=0


app = Flask(__name__)


def brandlist(operand2): #to get the list of index where brand name == operand
         index = 0
         li = []
         for i in db['brand']:
             if(i['name']==operand2):
                 li.append(index)
             index+=1
         return li




@app.route("/", methods = ['POST','GET'])
def query_get():
    
     if(request.method=='POST'):
         req_data = request.get_json()
         query_type = req_data['query_type']
         li = []
        
         for i in db['_id']: #getting the all the NAP id`s into a list for future use
                li.append(i['$oid'])
         index = 0
         liid = []

         if(query_type=="discounted_products_list"):
             operand1 = req_data['filters'][0]['operand1']
             operand2 = req_data['filters'][0]['operand2']
             operator = req_data['filters'][0]['operator']
                  
             if(operand1=="discount"):
                 global lidisc
                 lidisc = [] 
                 for i in db['price']: 
                        n = (i['regular_price']['value']-i['offer_price']['value'])/100
                        if(operator==">" and n>operand2):
                                lidisc.append(index)
                        elif(operator=="<" and n<operand2):
                                lidisc.append(index)
                        elif(operator=="==" and n == operand2):
                                lidisc.append(index)
                        index+=1
        
             elif(operand1=="brand.name"):
                    global libr
                    libr = brandlist(operand2)
                    
             else:
                 return "todo.."
      
             if(len(lidisc)>0 and len(libr)>0):    
                 for i in libr:
                     if(i in lidisc):
                         liid.append(li[i])
             elif(len(libr)>0):
                 for i in libr:
                     liid.append(li[i])       
             elif(len(lidisc)>0):
                 for i in lidisc:
                     liid.append(li[i]) 
             di = {"Discounted_product_list":liid}
                     
                     
         elif(query_type=="discounted_products_count|avg_discount"):
             operand1 = req_data['filters'][0]['operand1']
             operand2 = req_data['filters'][0]['operand2']
             operator = req_data['filters'][0]['operator']
                  
            
             if(operand1=="discount"):
                 global lidisc2
                 lidisc2 = [] 
                 for i in db['price']:
                        n = (i['regular_price']['value']-i['offer_price']['value'])
                        lidisc2.append(n)
             
             elif(operand1=="brand.name"):
                    global libr2
                    libr2 = brandlist(operand2)
 
             summ = 0.0
             count =0
             if(len(lidisc2)>0 and len(libr2)>0):
                 for i in libr2:
                        if(operator==">" and lidisc2[i]>operand2):
                                summ+=lidisc2[i]
                                count+=1
                        elif(operator=="<" and lidisc2[i]<operand2):
                                summ+=lidisc2[i]
                                count+=1
                        elif(operator=="==" and lidisc2[i] == operand2):
                                summ+=lidisc2[i]
                                count+=1
             elif(len(libr2)>0):
                 for i in libr2:
                     count+=1
             elif(len(lidisc2)>0):
                 for i in range(len(lidisc2)):
                        if(operator==">" and lidisc2[i]>operand2):
                                summ+=lidisc2[i]
                                count+=1
                        elif(operator=="<" and lidisc2[i]<operand2):
                                summ+=lidisc2[i]
                                count+=1
                        elif(operator=="==" and lidisc2[i] == operand2):
                                summ+=lidisc2[i]
                                count+=1
             try:
                 avg = summ/count
             except ZeroDivisionError:
                 avg = 0
             
             di = {"discounted_products_count":count,"avg_discount":avg}
             
            
         elif(query_type=="expensive_list"):
            
                  

            index=0
            indst = []
            basket = {}
            index = 0
            for i in db['price']:
                basket[li[index]] = int(i['basket_price']['value'])
                index+=1

            if(len(req_data)==2): 
             operand1 = req_data['filters'][0]['operand1']
             operand2 = req_data['filters'][0]['operand2']
             operator = req_data['filters'][0]['operator']
            
             if(operand1=="brand.name"):
                    indst = brandlist(operand2)             
            
            index = 0
            for i in db['similar_products']:
                    maximum = 0
                    for j in i['website_results']:
                        for k in i['website_results'][j]['knn_items']: 
                            if(maximum<k['_source']['price']['basket_price']['value']):
                                maximum = k['_source']['price']['basket_price']['value']
                    if(len(indst)>0):
                        if(index in indst):
                            if(maximum < basket[li[index]]):
                                liid.append(li[index])    
                    else:
                            if(maximum < basket[li[index]]):
                                liid.append(li[index])    
                    index+=1
                        
            di = {'expensive_list':liid}
             
         elif(query_type=="competition_discount_diff_list"):
            operand1 = req_data['filters'][0]['operand1']
            operand2 = req_data['filters'][0]['operand2']
            operator1 = req_data['filters'][0]['operator']
            operand4 = req_data['filters'][1]['operand2']
            
            basket = {}
            index=0
            for i in db['price']:
                basket[li[index]] = i['basket_price']['value']
                index+=1
            
            index = 0
            for i in db['similar_products']:
                    maxi = 0
                    for j in i['website_results']:
                            for k in i['website_results'][j]['knn_items']:
                                if(k['_source']['website_id'] == operand4):
                                        if(k['_source']['price']['basket_price']['value']>maxi):
                                            maxi = k['_source']['price']['basket_price']['value']
                    prt = maxi+((maxi/100)*operand2)
                    if(operator1==">" and basket[li[index]] > prt):
                                liid.append(li[index]) 
                    if(operator1=="<" and basket[li[index]] < prt):
                                liid.append(li[index])    
                    if(operator1=="==" and prt == basket[li[index]]):
                                liid.append(li[index])    
                    index+=1
            di = {"competition_discount_diff_list":liid}
            
     return di


if __name__ == "__main__":
    app.run(debug =True)

import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime

class Analysis:
    def __init__(self, base_format=None, df=None, use_keepa=True, delete_brands=False, lindo=False):
        self.base_format = deepcopy(base_format)
        self.df = deepcopy(df)
        self.base_format.drop(["Cost", "Raanana", "Target", "Avg(FQ>0)", "BB", "FBA Fee"], axis=1, inplace=True)
        self._use_keepa = use_keepa
        self._delete_brands = delete_brands
        self._skip = False
        self._name = None
        self._lindo = lindo
        
    def process_name_of_columns(self):
        '''
        Here we capitalize and lower names of the columns to make the file independent
        '''
        name_columns = {}
        right_name = {"qty": "qnty", "ean": "Barcode", "barcode": "Barcode", "stock": "qnty",
                      "offer": "price", "product name": "description", "item": "description", "ean code": "Barcode", 
                      "bezeichnung": "description", "menge": "qnty", "preis eur": "price", "ean barcode": "Barcode", 
                      "qty.": "qnty", "itemname": "description", "price usd.": "price", "nombre articulo": "description", 
                      "unidades": "qnty", "in stock": "qnty", "price (usd)": "price", "name": "description", 
                      "price (eur)": "price", "artikel": "description", "item description": "description", 
                      "net price": "price", "max qty": "qnty", "euro price": "price", "€ price": "price", 
                      "price eur": "price", "net price [eur]": "price", "qty available": "qnty", 
                      "ברקוד": "Barcode", "שם פריט": "description" , "מחיר": "price", "כמות במלאי": "qnty", 
                      "כמות": "qnty", "מלא": "qnty"}
        for column in self.df.columns:
            low_column = column.lower().strip()
            if low_column in right_name:
                name_columns[column] = right_name[low_column]
            else:
                name_columns[column] = low_column
        self.df = self.df.rename(name_columns, axis=1)
        
    def process_price(self):
        '''
        Here we fill all the NANs in price column
        '''
        if self.df["price"].isna().sum() == 0:
            return
        self.df["price"] = self.df["price"].fillna("no price")
            
    def delete_restricted_brands(self):
        """
        Here we delete brands that we cannot sell
        """
        if "brand" not in self.df.columns:
            if self._lindo is False:
                self.df["brand"] = self.df["description"].apply(lambda x: x.split()[0].lower().strip())
            else:
                self.df["brand"] = self.df["description"].apply(lambda x: x.split("-")[-1].lower().strip())
        self.brands = pd.read_excel(r"C:\Users\User\Desktop\Python for analysis\restriction_list.xlsx")
        res_brands = list(self.brands["brand"])
        del_idx = []
        for idx in self.df.index:
            brand = self.df.loc[idx, "brand"]
            brand = brand.lower().strip()
            if brand in res_brands:
                del_idx.append(idx)
        self.df = self.df[~self.df.index.isin(del_idx)]
        self.df = self.df.reset_index(drop=True)

    def convertation(self):
        """
        Here we convert the type of ean to int and asin to str
        """
        self.df['Barcode'] = pd.to_numeric(arg=self.df["Barcode"], errors="coerce", downcast="unsigned").astype("Int64")
        self.df = self.df.dropna(subset=["Barcode"])
        self.df.reset_index(inplace=True, drop=True)
        self.base_format["ASIN"] = self.base_format["ASIN"].astype("str")
        self.base_format["ASIN"] = self.base_format["ASIN"].apply(lambda x: x.strip())

    def sum_qnty(self):
        """
        Here we sum the qnty of the same barcodes
        """
        if self.df.shape[0] == self.df["Barcode"].nunique():
            return
        grouped = self.df.groupby(by=["Barcode"], as_index=False).agg({"description": "first", "qnty": "sum", "price": "min"})
        info = self.df.drop(["description", "qnty", "price"], axis=1).drop_duplicates(["Barcode"], keep="first")
        self.df = grouped.merge(info, on="Barcode", how="left")
        
    def delete_qnty(self, num=20):
        """
        Here we delete rows where quantity is less than num. Default 20
        """
        self.df["qnty"] = self.df["qnty"].fillna(-1)
        self.df["qnty_int"] = self.df["qnty"].apply(lambda x: int(x.strip("+")) if isinstance(x, str) else x)
        self.df = self.df[(self.df["qnty_int"] >= num) | (self.df["qnty_int"] == -1)]
        self.df = self.df.drop("qnty_int", axis=1)
        self.df.reset_index(inplace=True, drop=True)
        
    def merge_baseformat_and_df(self):
        '''
        Here we merge baseformat file with df. This will help us to see barcodes without asins
        '''
        self.base_format = self.base_format.merge(self.df, on="Barcode", how="left").dropna(subset="price")
        self.base_format.reset_index(drop=True, inplace=True)
        
    def get_missing_barcodes(self):
        """
        Here we get the barcodes that don't have asins in the base_format file
        """
        self.missing_barcodes = self.df[~self.df["Barcode"].isin(self.base_format["Barcode"])].reset_index(drop=True)
        self.missing_barcodes = self.missing_barcodes[["Barcode"]]
        if self.missing_barcodes.shape[0] == 0:
            self._skip = True
            return
        for barcode in self.missing_barcodes["Barcode"]:
            print(barcode, end=" ")
        print()
        
    def upload_keepa_barcodes(self):
        '''
        Here we upload the name of the keepa file with barcodes without asins in the base_format file
        '''
        name_file = input("Insert the name of the Keepa file of barcodes OR print 'skip': ")
        if name_file.lower().strip() == "skip":
            self._skip = True
            return 
        self.keepa_barcode = pd.read_excel(f'{name_file}.xlsx')

    def process_keepa_barcodes(self):
        '''
        Here we process data from keepa file
        '''
        self.keepa_barcode = self.keepa_barcode.rename({'Product Codes: EAN': 'Barcode'}, axis=1)
        if self.keepa_barcode["Barcode"].dtype == "object":
            self.keepa_barcode['Barcode'] = self.keepa_barcode['Barcode'].fillna("0").str.split(',').apply(lambda x: [int(ean) for ean in x])
            self.keepa_barcode = self.keepa_barcode.explode('Barcode')
            self.keepa_barcode['Barcode'] = self.keepa_barcode['Barcode'].astype('Int64')
        self.keepa_barcode = self.keepa_barcode[self.keepa_barcode["Barcode"].isin(self.missing_barcodes["Barcode"])].reset_index(drop=True)
    
    def concat_keepa_barcodes(self):
        '''
        Here we add barcodes and asins from keepa file to baseformat file
        '''
        self.base_format = self.base_format[~self.base_format["prohibited"].isin([1])] #delete restricted items
        self.base_format = pd.concat([self.base_format, self.keepa_barcode[["Barcode", "ASIN"]]], ignore_index=True)
        self.base_format = self.base_format.drop_duplicates(subset=["Barcode", "ASIN"])
    
    def get_asins(self):
        '''
        Here we get all the asins. Then we need to put them to keepa
        '''
        print("###" * 20)
        print("###" * 20)
        for asin in self.base_format["ASIN"].unique():
            print(asin, end=" ")
            
    def upload_keepa_asins(self):
        '''
        Here we upload the name of the keepa file with asins to get the BB and FBA Fee
        '''
        name_file = input("Insert the name of the Keepa file of ASINs: ")
        self.keepa_asin = pd.read_excel(f'{name_file}.xlsx')
        
    def process_keepa_asins(self):
        '''
        Here we process data from keepa file to get the BB and FBA Fee, delete bad BSR
        '''
        self.keepa_asin = self.keepa_asin.rename({'Product Codes: EAN': 'Barcode'}, axis=1)
        self.keepa_asin = self.keepa_asin[self.keepa_asin["Sales Rank: 30 days avg."] < 250_000]
        self.keepa_asin = self.keepa_asin[self.keepa_asin["Sales Rank: Current"] < 250_000]
        self.keepa_asin["BB"] = self.keepa_asin[["Buy Box: Current", "Buy Box: 30 days avg.", "Buy Box: 90 days avg."]].min(axis=1)
        self.keepa_asin = self.keepa_asin.dropna(subset=["BB", "FBA Fees:"])
        self.keepa_asin = self.keepa_asin.reset_index(drop=True)
        
    def merge_baseformat_and_asins(self):
        '''
        Here we merge baseformat file with asin file to add BB and FBA Fee
        '''
        self.base_format = self.base_format.merge(self.keepa_asin[["ASIN", "FBA Fees:", "BB"]], how="left", on="ASIN")
        self.base_format = self.base_format.dropna(subset="BB").reset_index(drop=True)
        
    def final_formatting(self):
        '''
        Here we make formatting so the file will be ready to anaylize
        '''
        for idx in self.base_format.index:
            sku = self.base_format.loc[idx, "SKU"]
            if sku is np.nan:
                barcode = self.base_format.loc[idx, "Barcode"]
                self.base_format.loc[idx, "SKU"] = self.df[self.df["Barcode"] == barcode]["description"].iloc[0]   
        self.base_format = self.base_format.drop(["description", "qnty", "QNTY", "Description", 
                                                  "price", "prohibited", "Value"], axis=1, errors="ignore")
        self.base_format = self.base_format.merge(self.df.drop("description", axis=1), on="Barcode", how="left")
        self.base_format = self.base_format.rename({"SKU": "description"}, axis=1)
        description = self.base_format.pop("description")
        price = self.base_format.pop("price")
        fba = self.base_format.pop("FBA Fees:")
        bb = self.base_format.pop("BB")
        self.base_format.insert(2, "Description", description)
        self.base_format.insert(4, "Price", price)
        self.base_format.insert(5, "FBA Fee", fba)
        self.base_format.insert(6, "BB", bb)
        self.base_format.insert(11, "Target", None)
        self.base_format.insert(13, "Cost", None)
        if "qnty" in self.base_format.columns:
            qnty = self.base_format.pop("qnty")
            self.base_format.insert(4, "Qnty", qnty)
    
    def remove_duplictated_columns(self):
        '''
        Here we delete duplicated columns if there are some
        '''
        delete_columns = list(filter(lambda x: x.endswith("_x"), self.base_format.columns))
        if delete_columns:
            self.base_format = self.base_format.drop(delete_columns, axis=1)
            rename_columns = {column: column.replace("_y", "") for column in self.base_format.columns if column.endswith("_y")}
            self.base_format = self.base_format.rename(rename_columns, axis=1)            
        
    def name_of_file(self):
        '''
        Here we ask a user what name of the file does he want
        '''
        print("###" * 20)
        print("###" * 20)
        name = input("What name of the file do you want?: ")
        self._name = f'{name} {datetime.now().strftime("%d.%m.%Y")}'
    
    def save_file(self):
        '''
        Here we save the file to excel
        '''
        try:
            self.base_format.to_excel(f'{[self._name, "FINAL"][self._name is None]}.xlsx', index=False)
            return
        except PermissionError:
            print("I can't save the file because it is already opened. Please, close the file!!!")
            name  = input("If you have closed the file, type 'yes': ")
            if name.strip().lower() == "yes":
                self.save_file()
  
    def start(self):
        """
        Start the whole process
        """
        self.process_name_of_columns()
        self.process_price()
        if self._delete_brands == True:
            self.delete_restricted_brands()
        self.convertation()
        if "qnty" in self.df.columns:
            self.sum_qnty()
            self.delete_qnty(num=20)
        self.merge_baseformat_and_df()
        if self._use_keepa == True:
            self.get_missing_barcodes()
            if self._skip == False:
                self.upload_keepa_barcodes()
                if self._skip == False:
                    self.process_keepa_barcodes()
                    self.concat_keepa_barcodes()
        self.get_asins()
        self.upload_keepa_asins()
        self.process_keepa_asins()
        self.merge_baseformat_and_asins()
        self.final_formatting()
        self.remove_duplictated_columns()
        self.name_of_file()
        self.save_file()
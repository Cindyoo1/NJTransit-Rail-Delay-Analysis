import pandas as pd

def clean_df(df)->pd.DataFrame:
    df = pd.read_csv("RAIL_NEC_CANCELLATIONS_DATA.csv")
    df.columns = df.columns.str.strip() # be gone white space
    df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    df = df[~df.apply(lambda row: row.astype(str).str.contains('-----').any(), axis=1)]


    cols_to_convert = ['CANCEL_COUNT', 'CANCEL_TOTAL', 'CANCEL_PERCENTAGE', 'YEAR']
    df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

    return df

def groupby_year(df)-> pd.DataFrame:
    grouped = df.groupby(['YEAR', 'CATEGORY']).agg(
        category_total = ('CANCEL_COUNT', 'sum')
    ).reset_index()

    grouped = grouped.sort_values(['YEAR','category_total'], ascending=[True,False])
    yearly_total = grouped.groupby('YEAR')['category_total'].sum().reset_index()
    yearly_total = yearly_total.rename(columns={'category_total': 'yearly_total'}) 
    grouped = grouped.merge(yearly_total, on='YEAR')

    grouped['category_percent'] = round((grouped['category_total'] / grouped['yearly_total']) * 100,2)
    grouped['annual_change'] = grouped.groupby('CATEGORY')['category_total'].pct_change().fillna(0).mul(100).round(2)
    
    return grouped

def top_categories(df)->pd.DataFrame:
    cat_dict = {}
    for year in df['YEAR'].unique():
        top_cats = df.loc[df['YEAR']==year, 'CATEGORY'].head(3).tolist()
        cat_dict[year] = top_cats

    return cat_dict

def main():
    df = pd.read_csv("RAIL_NEC_CANCELLATIONS_DATA.csv")
    df = clean_df(df)

    yearly_cancellation = groupby_year(df)
    yearly_cancellation.to_csv('Annual_cancellation_DATA.csv', index=False)

    top_cats = top_categories(yearly_cancellation)
    top_cats_df = pd.DataFrame.from_dict(top_cats, orient='index', columns=['Top_1', 'Top_2', 'Top_3'])
    top_cats_df.index.name = 'YEAR'

    top_cats_df.to_csv('Annual_top_categories.csv', index=True)

if __name__ == "__main__":
    main()

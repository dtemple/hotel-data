import csv
import pandas as pd
from flask import Flask
from flask import render_template
app = Flask(__name__)

def get_csv():
    csv_path = './static/frontdata.csv'
    # pass in column names for the CSV
    cols = ['message_id', 'conversation_id', 'segment', 'direction', 'status', 'inbox', 'msg_date', 'reaction_time',
            'resolution_time', 'resp_time', 'assignee',
            'author', 'contact_name', 'contact_handle', 'to', 'cc', 'bcc', 'extract', 'tags']
    # Upload the file
    df = pd.read_csv(csv_path, names=cols)
    # REMOVE JUNK INBOXES
    data = df.loc[~df['inbox'].isin(
        ['SD App', 'Vendors', 'Arrivals', '02 - Reservations', 'Support (Front desks)', '01 - Payments', 'Arrivals-dev',
         'SMS: Demo Hotel'])]

    # Create a table with count of unique users + count of unique messages
    master = data.pivot_table(values=['contact_handle', 'message_id'], index=['inbox'],
                              aggfunc=lambda x: len(x.unique()))
    master.columns = ['total_guests', 'total_messages']

    # Get the inbound messages and then find them by inbox
    inbound_messages = data.loc[data['direction'] == 'Inbound']
    inbound_messages = inbound_messages.pivot_table(values=['message_id'], index=['inbox'],
                                                    aggfunc=lambda x: len(x.unique()))
    inbound_messages.columns = ['inbound_messages']

    # Merge
    master = pd.merge(master, inbound_messages, left_index=True, right_index=True)

    # Pivot to get the count of messages per guest
    # Remove guests with 3 or less
    # Pivot again to get the count, then change the column name
    # NOTE: CHANGED FROM CONVERSATION ID TO CONTACT HANDLE HERE
    guestsByMessageCount = data.pivot_table(values=['message_id'], index=['inbox', 'contact_handle'],
                                            aggfunc=lambda x: len(x.unique()))
    active_only = guestsByMessageCount.loc[guestsByMessageCount['message_id'] > 3]
    active_only.reset_index(inplace=True)  # resets the index to make all data into columns
    active_count = active_only.pivot_table(values=['contact_handle'], index=['inbox'],
                                           aggfunc=lambda x: len(x.unique()))
    active_count.columns = ['active_guests']

    # Merge
    master = pd.merge(master, active_count, left_index=True, right_index=True)

    # Most active guest, remove dupes
    guestnames = data.pivot_table(values=['message_id'], index=['inbox', 'contact_name'],
                                  aggfunc=lambda x: len(x.unique()))
    guestnames.sort_values('message_id', ascending=False, inplace=True)
    guestnames.reset_index(inplace=True)
    guest = guestnames.groupby('inbox').first()
    guest.columns = ['most_active_guest', 'longest_thread']
    # guest.drop('index', axis=1, inplace=True)

    # Merge
    master = pd.merge(master, guest, left_index=True, right_index=True)
    master.sort_values('total_guests', ascending=False, inplace=True)

    #csv_file = open(csv_path, 'r')
    #csv_obj = csv.DictReader(csv_file)
    #csv_list = df
    return master

@app.route("/")
def index():
    template = 'index.html'
    object_list = get_csv()
    #object_list = manipulate(csv_front)
    return render_template(template, tables=[object_list.to_html()],titles=['Data'])

# TODO Sort the file by total_guests ascending = False
# TODO Add CSS
# TODO Get the data more efficiently (either through the API or via upload)
# TODO Add the emails by searching for the tags


if __name__ == '__main__':
    # Fire up the Flask test server
    app.run(debug=True, use_reloader=True)
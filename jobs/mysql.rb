require 'mysql2'
require 'date'

SCHEDULER.every '2s' do
# Myql connection

  db = Mysql2::Client.new(:host => "rfd-db-qa.researchnow.com",
                          :username => "ravenfish_user", :password => "Q1f18a1sr#2015", :port => 3306, :database => "ravenfish" )

  # Mysql query
  # sql = "SELECT max(seen_milliseconds) as seen_milliseconds FROM ravenfish.aggregate_impressions"
  sql = "SELECT DISTINCT(seen_milliseconds)as seen_milliseconds FROM ravenfish.aggregate_impressions ORDER BY seen_milliseconds DESC LIMIT 2"
  # maxTime1 = Array.new
  # maxTime1 = []
  # Execute the query
  results = db.query(sql)
  a = [ ]
  results.each do |row|
    # puts row['seen_milliseconds']
    maxTime = DateTime.strptime((row['seen_milliseconds']/1000).to_s,'%s')
    a.push(maxTime)
  end
  #
  # puts a.length
  validate = "None"
  for i in 0..(a.length-2)

      if (Time.parse(a[i].to_s)-Time.parse(a[i+1].to_s))/3600 > 1.0 then
       validate = 'YES'
      end

  end
  # puts ('----')
  send_event('mysql', { text: "@"+a[0].to_s } )
  send_event('mysql1', {text: "missed hours from last 10hrs:-" +(validate) } )


end
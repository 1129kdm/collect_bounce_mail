namespace :collect_bounce_mail do
  require 'fileutils'
  require 'sisimai'
  require 'mysql2'
  require 'tmpdir'

  desc "bounceしたメールを収集するスクリプト。"
  task collect: :environment do |task|
    COLUMNS = %w(
      timestamp
      lhost
      rhost
      alias
      listid
      reason
      action
      subject
      messageid
      smtpagent
      softbounce
      smtpcommand
      destination
      senderdomain
      feedbacktype
      diagnosticcode
      deliverystatus
      timezoneoffset
      addresser
      recipient
    )

    argv = ARGV.first.dup
    argv.slice!(task.name)
    MAIL_DIR = YAML.load(argv).first

    def process(path, **options)
      Dir.mktmpdir do |tmpdir|
        FileUtils.mv(Dir["#{path}/*"], tmpdir)
        v = Sisimai.make(tmpdir, **options) || []
        v.each {|e| yield(e) }
      end
    end

    def insert(data)
      values = data.to_hash.values_at(*COLUMNS)
      addresseralias = data.addresser.alias
      addresseralias = data.addresser if addresseralias.empty?
      values << addresseralias
      columns = (COLUMNS + ['addresseralias', 'digest', 'created_at', 'updated_at']).join(?,)
      timestamp = values.shift
      values = (["FROM_UNIXTIME(#{timestamp})"] + values.map(&:inspect) + ['SHA1(recipient)', 'NOW()', 'NOW()']).join(?,)
      sql = "INSERT INTO bounce_mails (#{columns}) VALUES (#{values})"
      #mysql.query(sql)
    end

    #mysql = Mysql2::Client.new(host: 'localhost', username: 'root', database: 'sisito')
    process(MAIL_DIR) do |data|
      insert(data)
    end
  end
end

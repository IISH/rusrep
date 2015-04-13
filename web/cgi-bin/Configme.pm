package Configme;

use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

use utf8;
use DBI;
use Exporter;

$VERSION = 1.00;
@ISA = qw(Exporter);

@EXPORT = qw(
		loadconfig
		loaddbconfig
	   );

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    use open ':std', ':encoding(utf-8)';
    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;
	if ($str=~/^(\S+)\s*\=\s*(.+?)$/)
	{
	    my ($name, $value) = ($1, $2);
            $config{$name} = $value;
	}
    }
    close(conf);

    return %config;
}

sub loaddbconfig
{
    my ($dbh, $DEBUG) = @_;
    my %dbconfig;

    $sqlquery = "select name, value, lang from datasets.configuration";
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    # lang reserved for other languages
    while (my ($name, $value, $lang) = $sth->fetchrow_array())
    {
	$dbconfig{$name} = $value;
    }

    return %dbconfig;
}

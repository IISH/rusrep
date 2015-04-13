#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

use utf8;
use Configme;
use DBI;
my %dbconfig = loadconfig("$Bin/../config/russianrep.config");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

$DEBUG = 1;
foreach $name (keys %dbconfig)
{
    if ($name=~/^(.+?)\_rus/)
    {
	my $default = $1;
	print "$name => $dbconfig{$name}\n" if ($DEBUG);
	sync_config($dbh, $name, $dbconfig{$name}, 'rus');
	print "$default => $dbconfig{$default}\n" if ($DEBUG);
 	sync_config($dbh, $default, $dbconfig{$default}, 'eng');
    }
}

sub sync_config
{
    my ($dbh, $name, $value, $lang) = @_;

    $dbh->do("delete from datasets.configuration where name='$name'");
    $dbh->do("insert into datasets.configuration (name, value, lang) values ('$name', '$value', '$lang')"); 

    return;
}

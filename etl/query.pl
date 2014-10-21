#!/usr/bin/perl
#
# Copyright (C) 2014 International Institute of Social History.
# @author Vyacheslav Tykhonov <vty@iisg.nl>
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use DBI;
my %dbconfig = loadconfig("$Bin/../config/russianrep.conf");
$dir = $dbconfig{path};
$path = $dbconfig{path};
$database = $dbconfig{maindb};
$dir=~s/\s+$//g;

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

use Getopt::Long;
$DIR = $Bin || "/home/www/clio_infra/clioinfra";

GetOptions (\%h);

my $result = GetOptions(
    \%options,
    'histclass1' => \$histclass1,
    'histclass2' => \$histclass2,
    'histclass3' => \$histclass3,
    'year' => \$year,
    'territory' => \$territory,
    'indicator' => \$indicator,
    'all', 'help',
    'debug=i' => \$DEBUG
);

print "$result\n";
exit(0);
query($dbh, $DEBUG);

sub query
{
    my ($dbh, $DEBUG, %params) = @_;

    $params = "histclass1='мужчины' and histclass2='только в городах' and year='1897'"; # and territory='Акмолинская'";
    $fields = "indicator_id, id, territory, ter_code, town, district, year, month, value,  value_unit, value_label, datatype, histclass1, histclass2, histclass3, histclass4, class1, class2, class3, class4, comment_source, source, volume, page, naborshik_id, comment_naborschik, indicator";

    $sqlquery = "select * from $database where 1=1"; # histclass1='мужчины' and histclass2='только в городах' and year='1897' and territory='Акмолинская'
    $sqlquery.= " and $params" if ($params);

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my @datarow = $sth->fetchrow_array())
    {
        print "@datarow\n";;
    };
    
};

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;
        my ($name, $value) = split(/\s*\=\s*/, $str);
        $config{$name} = $value;
    }
    close(conf);

    return %config;
}


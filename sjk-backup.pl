#!/usr/bin/perl


use strict;
use warnings;

use Config::Scoped;
use File::Copy;
use File::Rsync;
use File::Path qw(remove_tree);
use Parallel::ForkManager;


################################################################################
#### GLOBAL VARIABLES
################################################################################

# Reference to config hash
my $config;

# Verbosity level (log_level in config)
my $verbose;

# Directory containing lock/pid files.
my $lock_directory;


################################################################################
#### SJK-BACKUP
################################################################################

# Read the configuration file.
$config = read_conf();

# Run various checks on config.
check_config();

$verbose = $config->{'general'}{'log_level'};
$lock_directory = strip_trailing_slash($config->{'general'}{'lock_directory'});

# Do the backups.
do_backups($config);


################################################################################
#### FUNCTIONS
################################################################################

# Rotate backups (and delete oldest)
sub rotate_backups {
	my $name = shift;
	my $max = $config->{'backup'}{$name}{'number_of_backups'};
	my $backup_root = $config->{'general'}{'backup_root'};
	my $backup = $backup_root."/".$name;
	
	for (my $i = $max; $i != -1; $i--) {
		my $j = $i + 1;

		if (-d "$backup.$i" && $i == $max) {
			remove_tree("$backup.$i", { verbose => 0 }) or die "Could not remove_tree $backup.$i: $!";
			next;
		}

		if (-d "$backup.$i") {
			move("$backup.$i", "$backup.$j") or die "Could not move $backup.$i to $backup.$j: $!";
		}
	}

}

# Creates lock file. Prints warning and returns 1 if file already exists.
# If the file doesn't exist it creates it and returns 0.
sub create_lock_file {
	my $name = shift;
	my $file = $lock_directory."/".$name.".lock";
	
	if (-e $file) {
		print_warning("$file does alread exist!", 1);
		return 1;
	}

	open my $fh, ">$file" or die "Could not create $file: $!";
	close $fh;

	return 0;
}

# Removes lock file. Prints warning and returns 1 if it does not succeed, 
# returns 0 otherwise.
sub remove_lock_file {
	my $name = shift;
	my $file = $lock_directory."/".$name.".lock";

	if (unlink($file) == 0) {
		print_warning("Could not remove $file: $!", 1);
		return 1;
	}

	return 0;
}

# Write pid to lock file.
sub write_pid {
	my ($name, $pid) = @_;
	
	my $file = $lock_directory."/".$name.".lock";
	if (!-e $file) {
		print_warning("$file does not exist.", 1);
		return 1;
	}

	open my $fh, ">$file" or die "Could not open $file for writing: $!";
	print $fh $pid;
	close $fh;

	return 0;
}

# Run checks on config.
sub check_config {
	check_backup_root($config->{'general'}->{'backup_root'});
	check_lock_directory($config->{'general'}->{'lock_directory'});
}

# Returns a reference to a structure holding the config data.
sub read_conf {
	my $parser = new Config::Scoped file => 'sjk-backup.conf';
	$config = $parser->parse;
	return $config;
}

# Check if backup_root is defined, if it exists and is writable.
sub check_backup_root {
	my $root = shift;
	die "No backup_root defined" unless defined $config->{'general'}{'backup_root'};
	die "$root does not exist" unless -e $root;
	die "$root is not writable by us" unless -w $root;
	die "$root is not a directory" unless -d $root;
}

# Check if lock_directory is defined, if it exists and is writable.
sub check_lock_directory {
	my $directory = shift;

	die "No lock_directory defined" unless defined $config->{'general'}{'lock_directory'};
	die "$directory does not exist. Please create it." unless -d $directory;
	die "$directory is not writable" unless -w $directory;
}

# Prints warning if $level is <= $verbose (defined in config as log_level).
sub print_warning {
	my ($message, $level) = @_;
	
	if (!defined($verbose) || ($level <= $verbose)) {
		print STDERR "Warning: $message\n";
	}
}

# Print info if $level is <= $verbose (defined in config as log_level.
sub print_info {
	my ($message, $level) = @_;

	if (!defined($verbose) || ($level <= $verbose)) {
		print STDERR "Info: $message\n";
	}
}

# Iterate through the hosts that should be backed up and execute the parallel
# rsyncs.
sub do_backups {
	my $config = shift;
	my $pfm = new Parallel::ForkManager(4); # Four rsyncs at a time seems good.
	foreach my $key (keys %{$config->{'backup'}}) {
		# If the lock file already exists a sync is probably already going.
		next if create_lock_file($key); 

		my $pid = $pfm->start and next; # Fork!

		# Write the pid to the lock file.
		die if write_pid($key, $pid);
		backup_host($key, $config);

		remove_lock_file($key);
		$pfm->finish;
	}
	$pfm->wait_all_children;
}

sub backup_host {
	my ($name,$conf) = @_;
	my $hostconf = $conf->{'backup'}->{$name};

	my $user = $hostconf->{'user'};
	my $host = $hostconf->{'host'};
	my $backup_root = $conf->{'general'}->{'backup_root'};
	my $bwlimit = $hostconf->{'bwlimit'} ? $hostconf->{'bwlimit'} : 0;

	# Rotate backups first.
	rotate_backups($name);

	foreach my $path (@{$hostconf->{'path'}}) {

		$path = strip_trailing_slash($path);
		my $src = "$user\@$host:\"$path\"";
		my $dst = "$backup_root/$name.0";
		my $prev = "$backup_root/$name.1";

		my %settings = (
			'archive'			=> 1,
			'hard-links'		=> 1,
			'human-readable'	=> 1,
			'inplace'			=> 1,
			'numeric-ids'		=> 1,
			'delete'			=> 1,
			'delete-excluded'	=> 1,
			'relative'			=> 1,
			'acls'				=> 1,
			#	'xattr'				=> 1,
			'partial'			=> 1,
			#	'progress'			=> 1,
			#	'verbose'			=> 1,
			'link-dest'			=> [ $prev ]
		);

		$settings{'bwlimit'} = $bwlimit if defined $bwlimit;
		#$settings{'link-dest'} = $prev if -d $prev;

		print_info("Backing up $src to $dst", 1);

		my $rsync = File::Rsync->new(\%settings);
		$rsync->exec({
				src => $src,
				dest => $dst
		}) or print_warning("Rsync failed.", 1);
	}
}

# is_comment($line)
# Returns true if $line is a comment, and false if it is not.
sub is_comment {
	my $line = shift;
	$line =~ s/^(?:\s|\t)+//;
	if ($line =~ /^#/) {
		return 1;
	} else {
		return 0;
	}
}

# is_empty_line($line)
# Returns true if $line is empty, false if it isn't
sub is_empty_line {
	my $line = shift;
	if ($line =~ /^(?:\s|\t)*$/) {
		return 1;
	} else {
		return 0;
	}
}

sub strip_trailing_slash {
	my $line = shift;

	# Don't do anything if $line is a local or remote root filesystem.
	return $line if ($line eq '/');
	return $line if ($line =~ m,:/$,);

	# Otherwise strip trailing slash.
	$line =~ s,/+$,,;

	return $line;
}

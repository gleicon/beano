# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.define "golang" do |wot|
    wot.vm.box = "ubuntu/trusty64"
    wot.vm.network :private_network, ip: "192.168.33.21"

    wot.vm.provision "ansible" do |ansible| 
      ansible.playbook = "golang.yml"
    end 
  end

  config.vm.provider "virtualbox" do |v|
    v.customize ["modifyvm", :id, "--memory", "2048"]
    v.customize ["modifyvm", :id, "--cpus", "2"]
  end

end
